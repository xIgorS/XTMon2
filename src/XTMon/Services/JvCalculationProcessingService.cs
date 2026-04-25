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
    private const int HeartbeatMaxConsecutiveFailures = 3;
    private const int MarkStateMaxAttempts = 3;
    private const string StaleRunningJobErrorMessage = "JV background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JvCalculationProcessingService> _logger;
    private readonly JvCalculationOptions _jvCalculationOptions;
    private readonly TimeSpan _idleDelay;
    private readonly TimeSpan _heartbeatInterval;
    private readonly TimeSpan _markStateShutdownGrace;
    private readonly TimeSpan _markStateRetryDelay;
    private readonly JobCancellationRegistry _jobCancellationRegistry;

    public JvCalculationProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationProcessingService> logger,
        JobCancellationRegistry jobCancellationRegistry)
        : this(scopeFactory, jvCalculationOptions, logger, TimeSpan.FromSeconds(jvCalculationOptions.Value.ProcessorIdleDelaySeconds), null, jobCancellationRegistry)
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
        _markStateShutdownGrace = TimeSpan.FromSeconds(_jvCalculationOptions.ProcessorMarkStateShutdownGraceSeconds);
        _markStateRetryDelay = TimeSpan.FromSeconds(_jvCalculationOptions.ProcessorMarkStateRetryDelaySeconds);
        _jobCancellationRegistry = jobCancellationRegistry ?? new JobCancellationRegistry();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "JV calculation processing service started with StaleTimeoutSeconds={StaleTimeoutSeconds}, HeartbeatIntervalSeconds={HeartbeatIntervalSeconds}.",
            _jvCalculationOptions.JobRunningStaleTimeoutSeconds,
            _heartbeatInterval.TotalSeconds);

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
                    await WaitForNextPollOpportunityAsync(stoppingToken);
                    continue;
                }

                if (job is null)
                {
                    await WaitForNextPollOpportunityAsync(stoppingToken);
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

    private async Task WaitForNextPollOpportunityAsync(CancellationToken stoppingToken)
    {
        var idleDelayTask = Task.Delay(_idleDelay, stoppingToken);
        var cancellationSignalTask = _jobCancellationRegistry.WaitForJvJobCancellationAsync(stoppingToken);
        await Task.WhenAny(idleDelayTask, cancellationSignalTask);
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

            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Skipping JV job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            _logger.LogInformation("Processing JV job {JobId}, request {RequestType}, pnl date {PnlDate}.", job.JobId, job.RequestType, job.PnlDate);

            await repository.HeartbeatJvJobAsync(job.JobId, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Stopping JV job {JobId} after heartbeat because it is no longer active.", job.JobId);
                return;
            }

            heartbeatLoopCts = CancellationTokenSource.CreateLinkedTokenSource(jobCancellation.Token);
            heartbeatLoopTask = KeepHeartbeatAliveAsync(job.JobId, jobCancellation, heartbeatLoopCts.Token);

            string? queryFix = null;
            if (string.Equals(job.RequestType, "FixAndCheck", StringComparison.OrdinalIgnoreCase))
            {
                var fixTask = repository.FixJvCalculationAsync(job.PnlDate, executeCatchup: true, jobCancellation.Token);
                queryFix = await AwaitExecutionWithHeartbeatAsync(fixTask, heartbeatLoopTask, jobCancellation);
                if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
                {
                    _logger.LogInformation("Discarding JV fix result for job {JobId} because it is no longer active.", job.JobId);
                    return;
                }
            }

            var checkTask = repository.CheckJvCalculationAsync(job.PnlDate, jobCancellation.Token);
            var checkResult = await AwaitExecutionWithHeartbeatAsync(checkTask, heartbeatLoopTask, jobCancellation);
            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Discarding JV check result for job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            await repository.SaveJvJobResultAsync(job.JobId, checkResult.ParsedQuery, queryFix, checkResult.Table, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Skipping completion for JV job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId);
            heartbeatLoopCts = null;
            heartbeatLoopTask = null;

            var completed = await MarkJobTerminalAsync(
                job.JobId,
                (repository, ct) => repository.MarkJvJobCompletedAsync(job.JobId, ct),
                "mark-completed",
                cancellationToken);

            if (!completed)
            {
                _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed,
                    "JV job {JobId} finished execution but could not be marked Completed after {MaxAttempts} attempt(s); stale-job expiry will reclaim it.",
                    job.JobId, MarkStateMaxAttempts);
                return;
            }

            _logger.LogInformation("JV job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId);
            throw;
        }
        catch (OperationCanceledException) when (jobCancellation.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId);
            _logger.LogInformation("JV job {JobId} cancellation was requested.", job.JobId);
            await EnsureJvJobMarkedCancelledAsync(job.JobId, cancellationToken);
        }
        catch (Exception ex)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId);
            LogProcessorException(ex, $"job {job.JobId}");
            _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex, "JV job {JobId} failed.", job.JobId);

            await MarkJobTerminalAsync(
                job.JobId,
                (repository, ct) => repository.MarkJvJobFailedAsync(job.JobId, ex.Message, ct),
                "mark-failed",
                cancellationToken);
        }
        finally
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId);
            _jobCancellationRegistry.UnregisterJvJob(job.JobId, jobCancellation);
        }
    }

    private async Task KeepHeartbeatAliveAsync(long jobId, CancellationTokenSource jobCancellation, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
        using var timer = new PeriodicTimer(_heartbeatInterval);

        var consecutiveFailures = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    await repository.HeartbeatJvJobAsync(jobId, cancellationToken);
                    consecutiveFailures = 0;

                    if (await IsDbCancelObservedAsync(repository, jobId, cancellationToken))
                    {
                        _logger.LogInformation(
                            "JV job {JobId} is no longer Active in the database; requesting execution cancellation.",
                            jobId);
                        jobCancellation.Cancel();
                        return;
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    consecutiveFailures++;
                    LogProcessorException(ex, $"job {jobId} heartbeat");

                    if (consecutiveFailures >= HeartbeatMaxConsecutiveFailures)
                    {
                        _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex,
                            "Heartbeat failed {ConsecutiveFailures} consecutive times for JV job {JobId}; failing the job.",
                            consecutiveFailures, jobId);
                        throw;
                    }

                    _logger.LogWarning(ex,
                        "Heartbeat update failed for JV job {JobId} (consecutive failures: {ConsecutiveFailures}/{Max}). Will retry while the job remains active.",
                        jobId, consecutiveFailures, HeartbeatMaxConsecutiveFailures);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task<bool> IsDbCancelObservedAsync(IJvCalculationRepository repository, long jobId, CancellationToken cancellationToken)
    {
        var current = await repository.GetJvJobByIdAsync(jobId, cancellationToken);
        return current is not null && !MonitoringJobHelper.IsActiveStatus(current.Status);
    }

    private async Task<bool> MarkJobTerminalAsync(
        long jobId,
        Func<IJvCalculationRepository, CancellationToken, Task> action,
        string stage,
        CancellationToken outerToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(outerToken);
        cts.CancelAfter(_markStateShutdownGrace);

        for (var attempt = 1; attempt <= MarkStateMaxAttempts; attempt++)
        {
            if (cts.IsCancellationRequested)
            {
                _logger.LogWarning(
                    "Skipping remaining {Stage} attempts for JV job {JobId} because shutdown or grace period was reached; stale-job expiry will reclaim it.",
                    stage, jobId);
                return false;
            }

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
                await action(repository, cts.Token);
                return true;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning(
                    "{Stage} for JV job {JobId} was cancelled; stale-job expiry will reclaim it.",
                    stage, jobId);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(AppLogEvents.JvProcessorBackgroundFailed, ex,
                    "Failed {Stage} for JV job {JobId} (attempt {Attempt}/{MaxAttempts}).",
                    stage, jobId, attempt, MarkStateMaxAttempts);

                if (attempt < MarkStateMaxAttempts && !cts.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(_markStateRetryDelay, cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        return false;
                    }
                }
            }
        }

        return false;
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

    private static async Task StopHeartbeatLoopAsync(
        CancellationTokenSource? heartbeatLoopCts,
        Task? heartbeatLoopTask,
        ILogger logger,
        long jobId)
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
            logger.LogWarning(
                heartbeatLoopTask.Exception,
                "Heartbeat loop fault was swallowed while stopping JV calculation processing for job {JobId}.",
                jobId);
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

    private async Task EnsureJvJobMarkedCancelledAsync(long jobId, CancellationToken cancellationToken)
    {
        await MarkJobTerminalAsync(
            jobId,
            async (repository, ct) =>
            {
                if (!await IsJobActiveAsync(repository, jobId, ct))
                {
                    return;
                }

                await repository.MarkJvJobCancelledAsync(jobId, BackgroundJobCancellationService.JvJobCanceledMessage, ct);
            },
            "mark-cancelled",
            cancellationToken);
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
}
