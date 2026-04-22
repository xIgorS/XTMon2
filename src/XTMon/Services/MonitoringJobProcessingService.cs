using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class MonitoringJobProcessingService : BackgroundService
{
    private static readonly TimeSpan DefaultIdleDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan MarkStateShutdownGrace = TimeSpan.FromSeconds(10);
    private const string StaleRunningJobErrorMessage = "Monitoring background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MonitoringJobProcessingService> _logger;
    private readonly MonitoringJobsOptions _options;
    private readonly TimeSpan _idleDelay;
    private readonly JobCancellationRegistry _jobCancellationRegistry;

    public MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger,
        JobCancellationRegistry jobCancellationRegistry)
        : this(scopeFactory, options, logger, DefaultIdleDelay, jobCancellationRegistry)
    {
    }

    internal MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger,
        TimeSpan idleDelay,
        JobCancellationRegistry? jobCancellationRegistry = null)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _options = options.Value;
        _idleDelay = idleDelay;
        _jobCancellationRegistry = jobCancellationRegistry ?? new JobCancellationRegistry();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Monitoring job processing service started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                MonitoringJobRecord? job;

                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
                    var staleTimeout = TimeSpan.FromSeconds(_options.JobRunningStaleTimeoutSeconds);
                    var expiredCount = await repository.ExpireStaleRunningMonitoringJobsAsync(staleTimeout, StaleRunningJobErrorMessage, stoppingToken);
                    if (expiredCount > 0)
                    {
                        _logger.LogWarning("Marked {ExpiredCount} stale monitoring job(s) as failed.", expiredCount);
                    }

                    job = await repository.TryTakeNextMonitoringJobAsync(Environment.MachineName, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogProcessorException(ex, "processing loop poll");
                    _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex, "Monitoring job processing loop encountered a transient error. Retrying after delay.");
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
            _logger.LogInformation("Monitoring job processing service cancellation received.");
        }

        _logger.LogInformation("Monitoring job processing service stopped.");
    }

    private async Task ProcessJobAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        using var jobCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _jobCancellationRegistry.RegisterMonitoringJob(job.JobId, jobCancellation);

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
            var executors = scope.ServiceProvider.GetServices<IMonitoringJobExecutor>();
            var executor = executors.FirstOrDefault(candidate => candidate.CanExecute(job))
                ?? throw new InvalidOperationException($"No monitoring executor is registered for category '{job.Category}' and submenu '{job.SubmenuKey}'.");

            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Skipping monitoring job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            _logger.LogInformation("Processing monitoring job {JobId} for {Category}/{SubmenuKey}, pnl date {PnlDate}.", job.JobId, job.Category, job.SubmenuKey, job.PnlDate);

            await repository.HeartbeatMonitoringJobAsync(job.JobId, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Stopping monitoring job {JobId} after heartbeat because it is no longer active.", job.JobId);
                return;
            }

            var payload = await executor.ExecuteAsync(job, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Discarding monitoring job {JobId} result because the job was cancelled or otherwise finalized.", job.JobId);
                return;
            }

            await repository.SaveMonitoringJobResultAsync(job.JobId, payload, jobCancellation.Token);
            if (!await IsJobActiveAsync(repository, job.JobId, cancellationToken))
            {
                _logger.LogInformation("Skipping completion for monitoring job {JobId} because it is no longer active.", job.JobId);
                return;
            }

            await repository.MarkMonitoringJobCompletedAsync(job.JobId, jobCancellation.Token);

            _logger.LogInformation("Monitoring job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (jobCancellation.IsCancellationRequested)
        {
            _logger.LogInformation("Monitoring job {JobId} cancellation was requested.", job.JobId);
            await EnsureMonitoringJobMarkedCancelledAsync(job.JobId, cancellationToken);
        }
        catch (Exception ex)
        {
            LogProcessorException(ex, $"job {job.JobId}");
            _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex, "Monitoring job {JobId} failed.", job.JobId);

            using var markFailedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            markFailedCts.CancelAfter(MarkStateShutdownGrace);

            for (var attempt = 1; attempt <= 2; attempt++)
            {
                if (markFailedCts.IsCancellationRequested)
                {
                    _logger.LogWarning("Skipping remaining mark-failed attempts for monitoring job {JobId} because shutdown or grace period was reached; stale-job expiry will reclaim it.", job.JobId);
                    break;
                }

                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
                    await repository.MarkMonitoringJobFailedAsync(job.JobId, ex.Message, markFailedCts.Token);
                    break;
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Mark-failed for monitoring job {JobId} was cancelled; stale-job expiry will reclaim it.", job.JobId);
                    break;
                }
                catch (Exception markFailedException)
                {
                    _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, markFailedException,
                        "Failed to mark monitoring job {JobId} as failed (attempt {Attempt}/2).", job.JobId, attempt);
                    if (attempt < 2 && !markFailedCts.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(TimeSpan.FromSeconds(2), markFailedCts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }
            }
        }
        finally
        {
            _jobCancellationRegistry.UnregisterMonitoringJob(job.JobId, jobCancellation);
        }
    }

    private static async Task<bool> IsJobActiveAsync(IMonitoringJobRepository repository, long jobId, CancellationToken cancellationToken)
    {
        var currentJob = await repository.GetMonitoringJobByIdAsync(jobId, cancellationToken);
        return currentJob is not null && MonitoringJobHelper.IsActiveStatus(currentJob.Status);
    }

    private async Task EnsureMonitoringJobMarkedCancelledAsync(long jobId, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(MarkStateShutdownGrace);

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
            if (!await IsJobActiveAsync(repository, jobId, cts.Token))
            {
                return;
            }

            await repository.MarkMonitoringJobFailedAsync(jobId, BackgroundJobCancellationService.MonitoringJobCanceledMessage, cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Mark-cancelled for monitoring job {JobId} was interrupted; stale-job expiry will reclaim it.", jobId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Unable to mark monitoring job {JobId} as cancelled.", jobId);
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
            _logger.LogError(AppLogEvents.MonitoringProcessorSqlTimeout, sqlException,
                "Monitoring processor SQL timeout in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
            return;
        }

        if (SqlDataHelper.IsSqlConnectionFailure(sqlException))
        {
            _logger.LogError(AppLogEvents.MonitoringProcessorConnectionFailed, sqlException,
                "Monitoring processor SQL connection failure in {Context}. SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}.",
                context,
                sqlException.Number,
                sqlException.State,
                sqlException.Class);
        }
    }
}