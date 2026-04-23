using System.Diagnostics;
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
    private const long SlowPollStageThresholdMilliseconds = 1000;
    private const string StaleRunningJobErrorMessage = "Monitoring background job timed out while in Running status and was auto-failed.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MonitoringJobProcessingService> _logger;
    private readonly MonitoringJobsOptions _options;
    private readonly TimeSpan _idleDelay;
    private readonly TimeSpan _heartbeatInterval;
    private readonly JobCancellationRegistry _jobCancellationRegistry;

    public MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger,
        JobCancellationRegistry jobCancellationRegistry)
        : this(scopeFactory, options, logger, DefaultIdleDelay, null, jobCancellationRegistry)
    {
    }

    internal MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger,
        TimeSpan idleDelay,
        TimeSpan? heartbeatInterval = null,
        JobCancellationRegistry? jobCancellationRegistry = null)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _options = options.Value;
        _idleDelay = idleDelay;
        _heartbeatInterval = heartbeatInterval ?? BuildHeartbeatInterval(_options);
        _jobCancellationRegistry = jobCancellationRegistry ?? new JobCancellationRegistry();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Monitoring job processing service started with MaxConcurrentJobs={MaxConcurrentJobs}, PollIntervalSeconds={PollIntervalSeconds}, StaleTimeoutSeconds={StaleTimeoutSeconds}.",
            _options.MaxConcurrentJobs,
            _options.JobPollIntervalSeconds,
            _options.JobRunningStaleTimeoutSeconds);
        var activeJobs = new List<ActiveMonitoringJob>();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                PruneCompletedJobs(activeJobs);

                var claimedJobs = 0;

                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
                    var staleTimeout = TimeSpan.FromSeconds(_options.JobRunningStaleTimeoutSeconds);

                    int expiredCount;
                    try
                    {
                        expiredCount = await ExpireStaleRunningJobsAsync(repository, staleTimeout, stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        LogProcessorException(ex, "processing loop stale-expiry");
                        _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex,
                            "Monitoring job processing loop failed during stale-job expiry. Retrying after delay.");
                        await Task.Delay(_idleDelay, stoppingToken);
                        continue;
                    }

                    if (expiredCount > 0)
                    {
                        _logger.LogWarning("Marked {ExpiredCount} stale monitoring job(s) as failed.", expiredCount);
                    }

                    while (!stoppingToken.IsCancellationRequested && CountDispatchActiveJobs(activeJobs) < _options.MaxConcurrentJobs)
                    {
                        MonitoringJobRecord? job;
                        var preferredExcludedCategories = BuildPreferredExcludedCategories(activeJobs);
                        var hardExcludedCategories = BuildHardExcludedCategories(activeJobs);

                        try
                        {
                            job = await TryTakeNextMonitoringJobAsync(repository, preferredExcludedCategories, hardExcludedCategories, stoppingToken);
                        }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            LogProcessorException(ex, "processing loop take-next");
                            _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex,
                                "Monitoring job processing loop failed while claiming the next job. Retrying after delay.");
                            await Task.Delay(_idleDelay, stoppingToken);
                            break;
                        }

                        if (job is null)
                        {
                            break;
                        }

                        activeJobs.Add(new ActiveMonitoringJob(job, RunJobSafelyAsync(job, stoppingToken)));
                        claimedJobs++;
                    }
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

                if (claimedJobs > 0 || activeJobs.Count > 0)
                {
                    var dispatchActiveJobs = CountDispatchActiveJobs(activeJobs);
                    _logger.LogDebug(
                        "Monitoring processor cycle claimed {ClaimedJobs} job(s); {DispatchActiveJobs} dispatch-active job(s) and {TrackedJobs} tracked job(s) are present out of {MaxConcurrentJobs} slot(s).",
                        claimedJobs,
                        dispatchActiveJobs,
                        activeJobs.Count,
                        _options.MaxConcurrentJobs);
                }

                await WaitForNextDispatchOpportunityAsync(activeJobs, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Monitoring job processing service cancellation received.");
        }
        finally
        {
            await DrainActiveJobsAsync(activeJobs);
        }

        _logger.LogInformation("Monitoring job processing service stopped.");
    }

    private async Task RunJobSafelyAsync(MonitoringJobRecord job, CancellationToken stoppingToken)
    {
        try
        {
            await ProcessJobAsync(job, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task<int> ExpireStaleRunningJobsAsync(IMonitoringJobRepository repository, TimeSpan staleTimeout, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            return await repository.ExpireStaleRunningMonitoringJobsAsync(staleTimeout, StaleRunningJobErrorMessage, cancellationToken);
        }
        finally
        {
            LogSlowPollStage("stale-expiry", stopwatch.ElapsedMilliseconds);
        }
    }

    private async Task<MonitoringJobRecord?> TryTakeNextMonitoringJobAsync(
        IMonitoringJobRepository repository,
        IReadOnlyCollection<string> preferredExcludedCategories,
        IReadOnlyCollection<string> hardExcludedCategories,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var prioritizedExcludedCategories = preferredExcludedCategories
                .Concat(hardExcludedCategories)
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            if (prioritizedExcludedCategories.Length > 0)
            {
                var preferredJob = await repository.TryTakeNextMonitoringJobAsync(Environment.MachineName, prioritizedExcludedCategories, cancellationToken);
                if (preferredJob is not null)
                {
                    return preferredJob;
                }
            }

            if (hardExcludedCategories.Count > 0)
            {
                return await repository.TryTakeNextMonitoringJobAsync(Environment.MachineName, hardExcludedCategories, cancellationToken);
            }

            return await repository.TryTakeNextMonitoringJobAsync(Environment.MachineName, cancellationToken);
        }
        finally
        {
            LogSlowPollStage("take-next", stopwatch.ElapsedMilliseconds);
        }
    }

    private async Task ProcessJobAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
    {
        using var jobCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _jobCancellationRegistry.RegisterMonitoringJob(job.JobId, jobCancellation);
        CancellationTokenSource? heartbeatLoopCts = null;
        Task? heartbeatLoopTask = null;

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

            heartbeatLoopCts = CancellationTokenSource.CreateLinkedTokenSource(jobCancellation.Token);
            heartbeatLoopTask = KeepHeartbeatAliveAsync(job.JobId, heartbeatLoopCts.Token);

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

            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            heartbeatLoopCts = null;
            heartbeatLoopTask = null;

            await repository.MarkMonitoringJobCompletedAsync(job.JobId, jobCancellation.Token);

            _logger.LogInformation("Monitoring job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            throw;
        }
        catch (OperationCanceledException) when (jobCancellation.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            _logger.LogInformation("Monitoring job {JobId} cancellation was requested.", job.JobId);
            await EnsureMonitoringJobMarkedCancelledAsync(job.JobId, cancellationToken);
        }
        catch (Exception ex)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
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
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask);
            _jobCancellationRegistry.UnregisterMonitoringJob(job.JobId, jobCancellation);
        }
    }

    private async Task KeepHeartbeatAliveAsync(long jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        using var timer = new PeriodicTimer(_heartbeatInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    await repository.HeartbeatMonitoringJobAsync(jobId, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogProcessorException(ex, $"job {jobId} heartbeat");
                    _logger.LogWarning(ex, "Heartbeat update failed for monitoring job {JobId}. Will retry while the job remains active.", jobId);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
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
        finally
        {
            heartbeatLoopCts.Dispose();
        }
    }

    private static TimeSpan BuildHeartbeatInterval(MonitoringJobsOptions options)
    {
        var seconds = Math.Clamp(options.JobRunningStaleTimeoutSeconds / 3, 5, 30);
        return TimeSpan.FromSeconds(seconds);
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

    private async Task WaitForNextDispatchOpportunityAsync(IReadOnlyCollection<ActiveMonitoringJob> activeJobs, CancellationToken stoppingToken)
    {
        if (activeJobs.Count == 0)
        {
            await Task.Delay(_idleDelay, stoppingToken);
            return;
        }

        var nextActiveJob = Task.WhenAny(activeJobs.Select(activeJob => activeJob.Task));
        var idleDelayTask = Task.Delay(_idleDelay, stoppingToken);
        var cancellationSignalTask = _jobCancellationRegistry.WaitForMonitoringJobCancellationAsync(stoppingToken);
        await Task.WhenAny(nextActiveJob, idleDelayTask, cancellationSignalTask);
    }

    private async Task DrainActiveJobsAsync(IReadOnlyCollection<ActiveMonitoringJob> activeJobs)
    {
        if (activeJobs.Count == 0)
        {
            return;
        }

        try
        {
            await Task.WhenAll(activeJobs.Select(activeJob => activeJob.Task));
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void PruneCompletedJobs(List<ActiveMonitoringJob> activeJobs)
    {
        for (var index = activeJobs.Count - 1; index >= 0; index--)
        {
            var activeJob = activeJobs[index];
            if (!activeJob.Task.IsCompleted)
            {
                continue;
            }

            if (activeJob.Task.IsFaulted)
            {
                _logger.LogError(activeJob.Task.Exception, "Monitoring job worker task faulted unexpectedly.");
            }

            activeJobs.RemoveAt(index);
        }
    }

    private IReadOnlyCollection<string> BuildPreferredExcludedCategories(IReadOnlyCollection<ActiveMonitoringJob> activeJobs)
    {
        var dispatchActiveJobs = GetDispatchActiveJobs(activeJobs);
        if (dispatchActiveJobs.Count == 0)
        {
            return Array.Empty<string>();
        }

        return dispatchActiveJobs
            .Select(activeJob => activeJob.Job.Category)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    private IReadOnlyCollection<string> BuildHardExcludedCategories(IReadOnlyCollection<ActiveMonitoringJob> activeJobs)
    {
        var dispatchActiveJobs = GetDispatchActiveJobs(activeJobs);
        if (dispatchActiveJobs.Count == 0 || _options.CategoryMaxConcurrentJobs.Count == 0)
        {
            return Array.Empty<string>();
        }

        return dispatchActiveJobs
            .GroupBy(activeJob => activeJob.Job.Category, StringComparer.Ordinal)
            .Where(group =>
                _options.CategoryMaxConcurrentJobs.TryGetValue(group.Key, out var categoryLimit)
                && group.Count() >= categoryLimit)
            .Select(group => group.Key)
            .ToArray();
    }

    private int CountDispatchActiveJobs(IReadOnlyCollection<ActiveMonitoringJob> activeJobs)
    {
        return GetDispatchActiveJobs(activeJobs).Count;
    }

    private List<ActiveMonitoringJob> GetDispatchActiveJobs(IReadOnlyCollection<ActiveMonitoringJob> activeJobs)
    {
        if (activeJobs.Count == 0)
        {
            return [];
        }

        return activeJobs
            .Where(activeJob => !_jobCancellationRegistry.IsMonitoringJobCancellationRequested(activeJob.Job.JobId))
            .ToList();
    }

    private sealed record ActiveMonitoringJob(MonitoringJobRecord Job, Task Task);

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

    private void LogSlowPollStage(string stage, long elapsedMilliseconds)
    {
        if (elapsedMilliseconds < SlowPollStageThresholdMilliseconds)
        {
            return;
        }

        _logger.LogWarning(
            "Monitoring job processing poll stage {Stage} took {ElapsedMs} ms.",
            stage,
            elapsedMilliseconds);
    }
}
