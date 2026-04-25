using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public class MonitoringJobProcessingService : BackgroundService
{
    private const long SlowPollStageThresholdMilliseconds = 1000;
    private const string StaleRunningJobErrorMessage = "Monitoring background job timed out while in Running status and was auto-failed.";
    private const int HeartbeatMaxConsecutiveFailures = 3;
    private const int MarkStateMaxAttempts = 3;

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly MonitoringJobsOptions _options;
    private readonly TimeSpan _idleDelay;
    private readonly TimeSpan _heartbeatInterval;
    private readonly TimeSpan _markStateShutdownGrace;
    private readonly TimeSpan _markStateRetryDelay;
    private readonly JobCancellationRegistry _jobCancellationRegistry;
    private readonly int _maxConcurrentJobs;
    private readonly string _processorName;
    private readonly string[] _claimExcludedCategories;
    private readonly string[] _claimIncludedSubmenuKeys;
    private readonly string[] _claimExcludedSubmenuKeys;
    private readonly bool _isClaimScopedProcessor;

    public MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobProcessingService> logger,
        JobCancellationRegistry jobCancellationRegistry)
        : this(scopeFactory, options, logger, TimeSpan.FromSeconds(options.Value.ProcessorIdleDelaySeconds), null, jobCancellationRegistry)
    {
    }

    internal MonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger logger,
        TimeSpan idleDelay,
        TimeSpan? heartbeatInterval = null,
        JobCancellationRegistry? jobCancellationRegistry = null,
        string? processorName = null,
        IReadOnlyCollection<string>? ownedCategories = null,
        IReadOnlyCollection<string>? includedSubmenuKeys = null,
        IReadOnlyCollection<string>? excludedSubmenuKeys = null,
        int? maxConcurrentJobs = null)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _options = options.Value;
        _idleDelay = idleDelay;
        _heartbeatInterval = heartbeatInterval ?? BuildHeartbeatInterval(_options);
        _markStateShutdownGrace = TimeSpan.FromSeconds(_options.ProcessorMarkStateShutdownGraceSeconds);
        _markStateRetryDelay = TimeSpan.FromSeconds(_options.ProcessorMarkStateRetryDelaySeconds);
        _jobCancellationRegistry = jobCancellationRegistry ?? new JobCancellationRegistry();
        _maxConcurrentJobs = Math.Max(1, maxConcurrentJobs ?? _options.MaxConcurrentJobs);
        _processorName = string.IsNullOrWhiteSpace(processorName) ? nameof(MonitoringJobProcessingService) : processorName;
        _claimExcludedCategories = BuildClaimExcludedCategories(ownedCategories);
        _claimIncludedSubmenuKeys = BuildClaimSubmenuKeys(includedSubmenuKeys);
        _claimExcludedSubmenuKeys = BuildClaimSubmenuKeys(excludedSubmenuKeys);
        _isClaimScopedProcessor = _claimExcludedCategories.Length > 0 || _claimIncludedSubmenuKeys.Length > 0 || _claimExcludedSubmenuKeys.Length > 0;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "{ProcessorName} started with MaxConcurrentJobs={MaxConcurrentJobs}, PollIntervalSeconds={PollIntervalSeconds}, StaleTimeoutSeconds={StaleTimeoutSeconds}, HeartbeatIntervalSeconds={HeartbeatIntervalSeconds}.",
            _processorName,
            _maxConcurrentJobs,
            _options.JobPollIntervalSeconds,
            _options.JobRunningStaleTimeoutSeconds,
            _heartbeatInterval.TotalSeconds);
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

                    while (!stoppingToken.IsCancellationRequested && CountDispatchActiveJobs(activeJobs) < _maxConcurrentJobs)
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
                        "{ProcessorName} cycle claimed {ClaimedJobs} job(s); {DispatchActiveJobs} dispatch-active job(s) and {TrackedJobs} tracked job(s) are present out of {MaxConcurrentJobs} slot(s).",
                        _processorName,
                        claimedJobs,
                        dispatchActiveJobs,
                        activeJobs.Count,
                        _maxConcurrentJobs);
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

        _logger.LogInformation("{ProcessorName} stopped.", _processorName);
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
            if (_isClaimScopedProcessor)
            {
                return await repository.TryTakeNextMonitoringJobAsync(
                    Environment.MachineName,
                    _claimExcludedCategories,
                    _claimIncludedSubmenuKeys,
                    _claimExcludedSubmenuKeys,
                    cancellationToken);
            }

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
            var executors = scope.ServiceProvider.GetServices<IMonitoringJobExecutor>().ToList();
            var matchingExecutors = executors.Where(candidate => candidate.CanExecute(job)).ToList();
            if (matchingExecutors.Count > 1)
            {
                _logger.LogWarning(
                    "Multiple monitoring executors matched job {JobId} for {Category}/{SubmenuKey}: {ExecutorTypes}. Using the first match.",
                    job.JobId,
                    job.Category,
                    job.SubmenuKey,
                    string.Join(", ", matchingExecutors.Select(candidate => candidate.GetType().Name)));
            }

            var executor = matchingExecutors.FirstOrDefault()
                ?? throw new InvalidOperationException($"No monitoring executor is registered for category '{job.Category}' and submenu '{job.SubmenuKey}'.");

            var sqlExecutionContextAccessor = scope.ServiceProvider.GetRequiredService<SqlExecutionContextAccessor>();
            using var _ = sqlExecutionContextAccessor.BeginMonitoringJobScope(job);

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
            heartbeatLoopTask = KeepHeartbeatAliveAsync(job.JobId, jobCancellation, heartbeatLoopCts.Token);

            var executionTask = executor.ExecuteAsync(job, jobCancellation.Token);
            var payload = await AwaitExecutionWithHeartbeatAsync(executionTask, heartbeatLoopTask, jobCancellation);
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

            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId, _processorName);
            heartbeatLoopCts = null;
            heartbeatLoopTask = null;

            var completed = await MarkJobTerminalAsync(
                job.JobId,
                (repository, ct) => repository.MarkMonitoringJobCompletedAsync(job.JobId, ct),
                "mark-completed",
                cancellationToken);

            if (!completed)
            {
                _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed,
                    "Monitoring job {JobId} finished execution but could not be marked Completed after {MaxAttempts} attempt(s); stale-job expiry will reclaim it.",
                    job.JobId, MarkStateMaxAttempts);
                return;
            }

            _logger.LogInformation("Monitoring job {JobId} completed.", job.JobId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId, _processorName);
            throw;
        }
        catch (OperationCanceledException) when (jobCancellation.IsCancellationRequested)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId, _processorName);
            _logger.LogInformation("Monitoring job {JobId} cancellation was requested.", job.JobId);
            await EnsureMonitoringJobMarkedCancelledAsync(job.JobId, cancellationToken);
        }
        catch (Exception ex)
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId, _processorName);
            LogProcessorException(ex, $"job {job.JobId}");
            _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex, "Monitoring job {JobId} failed.", job.JobId);

            await MarkJobTerminalAsync(
                job.JobId,
                (repository, ct) => repository.MarkMonitoringJobFailedAsync(job.JobId, ex.Message, ct),
                "mark-failed",
                cancellationToken);
        }
        finally
        {
            await StopHeartbeatLoopAsync(heartbeatLoopCts, heartbeatLoopTask, _logger, job.JobId, _processorName);
            _jobCancellationRegistry.UnregisterMonitoringJob(job.JobId, jobCancellation);
        }
    }

    private async Task KeepHeartbeatAliveAsync(long jobId, CancellationTokenSource jobCancellation, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        using var timer = new PeriodicTimer(_heartbeatInterval);

        var consecutiveFailures = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    await repository.HeartbeatMonitoringJobAsync(jobId, cancellationToken);
                    consecutiveFailures = 0;

                    if (await IsDbCancelObservedAsync(repository, jobId, cancellationToken))
                    {
                        _logger.LogInformation(
                            "Monitoring job {JobId} is no longer Active in the database; requesting execution cancellation.",
                            jobId);
                        jobCancellation.Cancel();
                        return;
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (SqlException ex) when (IsTransientHeartbeatSqlFailure(ex))
                {
                    consecutiveFailures++;

                    if (consecutiveFailures >= HeartbeatMaxConsecutiveFailures)
                    {
                        _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex,
                            "Transient SQL heartbeat failure repeated {ConsecutiveFailures} consecutive times for monitoring job {JobId}; failing the job.",
                            consecutiveFailures, jobId);
                        throw;
                    }

                    if (consecutiveFailures == 1)
                    {
                        _logger.LogDebug(
                            "Transient SQL heartbeat failure for monitoring job {JobId}. The job will keep running and heartbeat will retry on the next interval.",
                            jobId);
                        continue;
                    }

                    _logger.LogWarning(ex,
                        "Transient SQL heartbeat failure repeated for monitoring job {JobId} (consecutive failures: {ConsecutiveFailures}/{Max}). Will retry while the job remains active.",
                        jobId, consecutiveFailures, HeartbeatMaxConsecutiveFailures);
                }
                catch (Exception ex)
                {
                    consecutiveFailures++;
                    LogProcessorException(ex, $"job {jobId} heartbeat");

                    if (consecutiveFailures >= HeartbeatMaxConsecutiveFailures)
                    {
                        _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex,
                            "Heartbeat failed {ConsecutiveFailures} consecutive times for monitoring job {JobId}; failing the job.",
                            consecutiveFailures, jobId);
                        throw;
                    }

                    _logger.LogWarning(ex,
                        "Heartbeat update failed for monitoring job {JobId} (consecutive failures: {ConsecutiveFailures}/{Max}). Will retry while the job remains active.",
                        jobId, consecutiveFailures, HeartbeatMaxConsecutiveFailures);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task<bool> IsDbCancelObservedAsync(IMonitoringJobRepository repository, long jobId, CancellationToken cancellationToken)
    {
        var current = await repository.GetMonitoringJobByIdAsync(jobId, cancellationToken);
        return current is not null && !MonitoringJobHelper.IsActiveStatus(current.Status);
    }

    private static bool IsTransientHeartbeatSqlFailure(SqlException ex)
    {
        return SqlDataHelper.IsSqlTimeout(ex)
            || SqlDataHelper.IsSqlLockTimeout(ex)
            || SqlDataHelper.IsSqlDeadlock(ex)
            || SqlDataHelper.IsSqlConnectionFailure(ex);
    }

    private async Task<bool> MarkJobTerminalAsync(
        long jobId,
        Func<IMonitoringJobRepository, CancellationToken, Task> action,
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
                    "Skipping remaining {Stage} attempts for monitoring job {JobId} because shutdown or grace period was reached; stale-job expiry will reclaim it.",
                    stage, jobId);
                return false;
            }

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
                await action(repository, cts.Token);
                return true;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning(
                    "{Stage} for monitoring job {JobId} was cancelled; stale-job expiry will reclaim it.",
                    stage, jobId);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(AppLogEvents.MonitoringProcessorBackgroundFailed, ex,
                    "Failed {Stage} for monitoring job {JobId} (attempt {Attempt}/{MaxAttempts}).",
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

    private static async Task<MonitoringJobResultPayload> AwaitExecutionWithHeartbeatAsync(
        Task<MonitoringJobResultPayload> executionTask,
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
        long jobId,
        string processorName)
    {
        if (heartbeatLoopCts is null || heartbeatLoopTask is null)
        {
            return;
        }

        try
        {
            heartbeatLoopCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

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
                "Heartbeat loop fault was swallowed while stopping {ProcessorName} for monitoring job {JobId}.",
                processorName,
                jobId);
        }
        finally
        {
            try
            {
                heartbeatLoopCts.Dispose();
            }
            catch (ObjectDisposedException)
            {
            }
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
        await MarkJobTerminalAsync(
            jobId,
            async (repository, ct) =>
            {
                if (!await IsJobActiveAsync(repository, jobId, ct))
                {
                    return;
                }

                await repository.MarkMonitoringJobCancelledAsync(jobId, BackgroundJobCancellationService.MonitoringJobCanceledMessage, ct);
            },
            "mark-cancelled",
            cancellationToken);
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
        if (_isClaimScopedProcessor)
        {
            return Array.Empty<string>();
        }

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
        if (_isClaimScopedProcessor)
        {
            return Array.Empty<string>();
        }

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

        return activeJobs.ToList();
    }

    private static string[] BuildClaimExcludedCategories(IReadOnlyCollection<string>? ownedCategories)
    {
        if (ownedCategories is null || ownedCategories.Count == 0)
        {
            return [];
        }

        var ownedCategorySet = ownedCategories
            .Where(category => !string.IsNullOrWhiteSpace(category))
            .Select(category => category.Trim())
            .ToHashSet(StringComparer.Ordinal);

        if (ownedCategorySet.Count == 0)
        {
            return [];
        }

        return MonitoringJobHelper.AllCategories
            .Where(category => !ownedCategorySet.Contains(category))
            .ToArray();
    }

    private static string[] BuildClaimSubmenuKeys(IReadOnlyCollection<string>? submenuKeys)
    {
        if (submenuKeys is null || submenuKeys.Count == 0)
        {
            return [];
        }

        return submenuKeys
            .Where(submenuKey => !string.IsNullOrWhiteSpace(submenuKey))
            .Select(submenuKey => submenuKey.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    private sealed record ActiveMonitoringJob(MonitoringJobRecord Job, Task Task);

    private void LogProcessorException(Exception ex, string context)
    {
        if (ex is not Microsoft.Data.SqlClient.SqlException sqlException)
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
