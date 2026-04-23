using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using Microsoft.Extensions.Options;
using Microsoft.Data.SqlClient;

namespace XTMon.Services;

public sealed class JvCalculationProcessingService : BackgroundService
{
    private static readonly TimeSpan DefaultIdleDelay = TimeSpan.FromSeconds(5);
    private const string StaleRunningJobErrorMessage = "JV background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JvCalculationProcessingService> _logger;
    private readonly JvCalculationOptions _jvCalculationOptions;
    private readonly TimeSpan _idleDelay;
    private readonly TimeSpan _heartbeatInterval;
    private readonly JobCancellationRegistry _jobCancellationRegistry;

    public JvCalculationProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationProcessingService> logger,
        JobCancellationRegistry jobCancellationRegistry)
        : this(scopeFactory, jvCalculationOptions, logger, DefaultIdleDelay, null, jobCancellationRegistry)
    {
    }

    internal JvCalculationProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationProcessingService> logger,
        TimeSpan idleDelay,
        TimeSpan? heartbeatInterval = null,
        JobCancellationRegistry? jobCancellationRegistry = null)
    {
        _scopeFactory = scopeFactory;
        _jvCalculationOptions = jvCalculationOptions.Value;
        _logger = logger;
        _idleDelay = idleDelay;
        _heartbeatInterval = heartbeatInterval ?? BuildHeartbeatInterval(_jvCalculationOptions);
        _jobCancellationRegistry = jobCancellationRegistry ?? new JobCancellationRegistry();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("JV calculation processing service started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                JvJobRecord? job;
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

                    var staleTimeout = TimeSpan.FromSeconds(_jvCalculationOptions.JobRunningStaleTimeoutSeconds);
                    var expiredCount = await repository.ExpireStaleRunningJobsAsync(staleTimeout, StaleRunningJobErrorMessage, stoppingToken);
                    if (expiredCount > 0)
                    {
                        _logger.LogWarning("Marked {ExpiredCount} stale JV running job(s) as failed.", expiredCount);
                    }

                    job = await repository.TryTakeNextJvJobAsync(Environment.MachineName, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogProcessorException(ex, "processing loop poll");
                    _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex, "JV calculation processing loop encountered a transient error. Retrying after delay.");
                    await Task.Delay(_idleDelay, stoppingToken);
                    continue;
                }

                if (job is null)
                {
                    await Task.Delay(_idleDelay, stoppingToken);
                    continue;
                }

                await ProcessJobAsync(job, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("JV calculation processing service cancellation received.");
        }

        _logger.LogInformation("JV calculation processing service stopped.");
    }

    private async Task ProcessJobAsync(JvJobRecord job, CancellationToken cancellationToken)
    {
        using var jobCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _jobCancellationRegistry.RegisterJvJob(job.JobId, jobCancellation);
        CancellationTokenSource? heartbeatLoopCts = null;
        Task? heartbeatLoopTask = null;

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

            if (!await IsJobActiveAsync(repository, job.JobId, CancellationToken.None))
            {
                _logger.LogInformation("Skipping JV job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            _logger.LogInformation("Processing JV job {JobId}, request {RequestType}, pnl date {PnlDate}.", job.JobId, job.RequestType, job.PnlDate);

            await repository.HeartbeatJvJobAsync(job.JobId, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, CancellationToken.None))
            {
                _logger.LogInformation("Stopping JV job {JobId} after heartbeat because it is no longer active.", job.JobId);
                return;
            }

            heartbeatLoopCts = CancellationTokenSource.CreateLinkedTokenSource(jobCancellation.Token);
            heartbeatLoopTask = KeepHeartbeatAliveAsync(job.JobId, heartbeatLoopCts.Token);

            string? queryFix = null;
            if (string.Equals(job.RequestType, "FixAndCheck", StringComparison.OrdinalIgnoreCase))
            {
                var fixTask = repository.FixJvCalculationAsync(job.PnlDate, executeCatchup: true, jobCancellation.Token);
                queryFix = await AwaitExecutionWithHeartbeatAsync(fixTask, heartbeatLoopTask, jobCancellation);
                if (!await IsJobActiveAsync(repository, job.JobId, CancellationToken.None))
                {
                    _logger.LogInformation("Discarding JV fix result for job {JobId} because it is no longer active.", job.JobId);
                    return;
                }
            }

            var checkTask = repository.CheckJvCalculationAsync(job.PnlDate, jobCancellation.Token);
            var checkResult = await AwaitExecutionWithHeartbeatAsync(checkTask, heartbeatLoopTask, jobCancellation);
            if (!await IsJobActiveAsync(repository, job.JobId, CancellationToken.None))
            {
                _logger.LogInformation("Discarding JV check result for job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            await repository.SaveJvJobResultAsync(job.JobId, checkResult.ParsedQuery, queryFix, checkResult.Table, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, CancellationToken.None))
            {
                _logger.LogInformation("Skipping completion for JV job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            heartbeatLoopCts = null;
            heartbeatLoopTask = null;

            await repository.MarkJvJobCompletedAsync(job.JobId, jobCancellation.Token);

            _logger.LogInformation("JV job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            throw;
        }
        catch (OperationCanceledException) when (jobCancellation.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            _logger.LogInformation("JV job {JobId} cancellation was requested.", job.JobId);
            await EnsureJvJobMarkedCancelledAsync(job.JobId);
        }
        catch (Exception ex)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            LogProcessorException(ex, $"job {job.JobId}");
            _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex, "JV job {JobId} failed.", job.JobId);
            for (var attempt = 1; attempt <= 2; attempt++)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
                    await repository.MarkJvJobFailedAsync(job.JobId, ex.Message, CancellationToken.None);
                    break;
                }
                catch (Exception markFailedException)
                {
                    _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, markFailedException,
                        "Failed to mark JV job {JobId} as failed (attempt {Attempt}/2).", job.JobId, attempt);
                    if (attempt < 2)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(2));
                    }
                }
            }
        }
        finally
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            _jobCancellationRegistry.UnregisterJvJob(job.JobId, jobCancellation);
        }
    }

    private async Task KeepHeartbeatAliveAsync(long jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
        using var timer = new PeriodicTimer(_heartbeatInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    await repository.HeartbeatJvJobAsync(jobId, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (SqlException ex) when (IsFatalHeartbeatException(ex))
                {
                    LogProcessorException(ex, $"job {jobId} heartbeat");
                    _logger.LogError(ex,
                        "Heartbeat update failed for JV job {JobId} with a non-retryable SQL error. Stopping the worker.",
                        jobId);
                    throw;
                }
                catch (Exception ex)
                {
                    LogProcessorException(ex, $"job {jobId} heartbeat");
                    _logger.LogWarning(ex, "Heartbeat update failed for JV job {JobId}. Will retry while the job remains active.", jobId);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task<T> AwaitExecutionWithHeartbeatAsync<T>(
        Task<T> executionTask,
        Task heartbeatLoopTask,
        CancellationTokenSource jobCancellation)
    {
        var completedTask = await Task.WhenAny(executionTask, heartbeatLoopTask);
        if (completedTask == executionTask)
        {
            return await executionTask;
        }

        try
        {
            await heartbeatLoopTask;
        }
        catch (Exception)
        {
            jobCancellation.Cancel();

            try
            {
                await executionTask;
            }
            catch (OperationCanceledException) when (jobCancellation.IsCancellationRequested)
            {
            }

            throw;
        }

        return await executionTask;
    }

    private static async Task StopHeartbeatLoopAsync(CancellationTokenSource? heartbeatLoopCts, Task? heartbeatLoopTask)
    {
        if (heartbeatLoopCts is null || heartbeatLoopTask is null)
        {
            return;
        }

        heartbeatLoopCts.Cancel();

        try
        {
            await heartbeatLoopTask;
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception) when (heartbeatLoopTask.IsFaulted)
        {
        }
        finally
        {
            heartbeatLoopCts.Dispose();
        }
    }

    private static async Task<bool> IsJobActiveAsync(IJvCalculationRepository repository, long jobId, CancellationToken cancellationToken)
    {
        var currentJob = await repository.GetJvJobByIdAsync(jobId, cancellationToken);
        return currentJob is not null && MonitoringJobHelper.IsActiveStatus(currentJob.Status);
    }

    private async Task EnsureJvJobMarkedCancelledAsync(long jobId)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
            if (!await IsJobActiveAsync(repository, jobId, CancellationToken.None))
            {
                return;
            }

            await repository.MarkJvJobCancelledAsync(jobId, BackgroundJobCancellationService.JvJobCanceledMessage, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Unable to mark JV job {JobId} as cancelled.", jobId);
        }
    }

    private void LogProcessorException(Exception ex, string context)
    {
        if (ex is not SqlException sqlException)
        {
            return;
        }

        if (SqlDataHelper.IsSqlTimeout(sqlException))
        {
            _logger.LogError(AppLogEvents.JvProcessorSqlTimeout, sqlException,
                "JV processor SQL timeout in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
            return;
        }

        if (SqlDataHelper.IsSqlConnectionFailure(sqlException))
        {
            _logger.LogError(AppLogEvents.JvProcessorConnectionFailed, sqlException,
                "JV processor SQL connection failure in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
        }
    }

    private static TimeSpan BuildHeartbeatInterval(JvCalculationOptions options)
    {
        var seconds = Math.Clamp(options.JobRunningStaleTimeoutSeconds / 3, 5, 30);
        return TimeSpan.FromSeconds(seconds);
    }

    private static bool IsFatalHeartbeatException(SqlException ex)
    {
        return SqlDataHelper.IsSqlTimeout(ex) || SqlDataHelper.IsSqlConnectionFailure(ex);
    }

}
