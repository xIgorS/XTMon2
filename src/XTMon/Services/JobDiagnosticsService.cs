using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class JobDiagnosticsService
{
    private const string MonitoringForceExpireMessage = "Monitoring background job was force-expired from the System Diagnostics stuck-jobs panel.";
    private const string JvForceExpireMessage = "JV background job was force-expired from the System Diagnostics stuck-jobs panel.";
    private const string ReplayForceExpireMessage = "Replay batch was force-expired from the System Diagnostics stuck-jobs panel.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<JobDiagnosticsService> _logger;
    private readonly MonitoringJobsOptions _monitoringOptions;
    private readonly JvCalculationOptions _jvOptions;
    private readonly ReplayFlowsOptions _replayOptions;

    public JobDiagnosticsService(
        IServiceScopeFactory scopeFactory,
        ILogger<JobDiagnosticsService> logger,
        IOptions<MonitoringJobsOptions> monitoringOptions,
        IOptions<JvCalculationOptions> jvOptions,
        IOptions<ReplayFlowsOptions> replayOptions)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _monitoringOptions = monitoringOptions.Value;
        _jvOptions = jvOptions.Value;
        _replayOptions = replayOptions.Value;
    }

    public async Task<StuckJobsReport> GetStuckJobsReportAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var monitoringRepository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var jvRepository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
        var replayRepository = scope.ServiceProvider.GetRequiredService<IReplayFlowRepository>();

        var monitoringThreshold = ComputeActivityThreshold(_monitoringOptions.JobRunningStaleTimeoutSeconds);
        var jvThreshold = ComputeActivityThreshold(_jvOptions.JobRunningStaleTimeoutSeconds);
        var replayThreshold = ComputeReplayActivityThreshold(_replayOptions.RunningStaleTimeoutSeconds);

        var monitoringStuck = await monitoringRepository.GetStuckMonitoringJobsAsync(monitoringThreshold, cancellationToken);
        var jvStuck = await jvRepository.GetStuckJvJobsAsync(jvThreshold, cancellationToken);
        var replayStuck = (await replayRepository.GetStuckReplayBatchesAsync(cancellationToken))
            .Where(row => row.AgeSeconds > replayThreshold.TotalSeconds)
            .ToArray();

        IReadOnlySet<long> monitoringActiveRuntimeJobIds = new HashSet<long>();
        try
        {
            monitoringActiveRuntimeJobIds = await monitoringRepository.GetRunningMonitoringJobIdsByDmvAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex,
                "Monitoring DMV runtime lookup failed while building the stuck-jobs report. Falling back to stale-activity monitoring rows.");
        }

        if (monitoringActiveRuntimeJobIds.Count > 0)
        {
            monitoringStuck = monitoringStuck
                .Where(job => !monitoringActiveRuntimeJobIds.Contains(job.JobId))
                .ToArray();
        }

        return new StuckJobsReport(monitoringStuck, jvStuck, replayStuck, DateTime.UtcNow);
    }

    public async Task<MonitoringProcessorHealthReport> GetMonitoringProcessorHealthReportAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var monitoringRepository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var activeJobs = await monitoringRepository.GetActiveMonitoringJobsAsync(cancellationToken);
        var queueBacklogGracePeriod = ComputeQueueBacklogGracePeriod(_monitoringOptions.ProcessorIdleDelaySeconds);
        var nowUtc = DateTime.UtcNow;

        IReadOnlySet<long> liveRuntimeJobIds = new HashSet<long>();
        var dmvAvailable = true;
        try
        {
            liveRuntimeJobIds = await monitoringRepository.GetRunningMonitoringJobIdsByDmvAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex,
                "Monitoring DMV runtime lookup failed while building the processor health report. The live worker check will be marked unavailable.");
            dmvAvailable = false;
        }

        var rows = MonitoringJobHelper.AllCategories
            .Select(category => BuildMonitoringProcessorHealthRow(
                category,
                activeJobs.Where(job => string.Equals(job.Category, category, StringComparison.OrdinalIgnoreCase)).ToArray(),
                liveRuntimeJobIds,
                dmvAvailable,
                queueBacklogGracePeriod,
                nowUtc))
            .ToArray();

        return new MonitoringProcessorHealthReport(rows, nowUtc, queueBacklogGracePeriod, dmvAvailable);
    }

    public async Task<ForceExpireResult> ForceExpireAllStuckAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var monitoringRepository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var jvRepository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();
        var replayRepository = scope.ServiceProvider.GetRequiredService<IReplayFlowRepository>();

        var monitoringThreshold = ComputeActivityThreshold(_monitoringOptions.JobRunningStaleTimeoutSeconds);
        var jvThreshold = ComputeActivityThreshold(_jvOptions.JobRunningStaleTimeoutSeconds);
        var replayThreshold = ComputeReplayActivityThreshold(_replayOptions.RunningStaleTimeoutSeconds);

        var monitoringExpired = await monitoringRepository.RecoverOrphanedMonitoringJobsAsync(monitoringThreshold, MonitoringForceExpireMessage, cancellationToken);
        var jvExpired = await jvRepository.ExpireStaleRunningJobsAsync(jvThreshold, JvForceExpireMessage, cancellationToken);
        var replayExpired = await replayRepository.FailStaleReplayBatchesAsync(replayThreshold, ReplayForceExpireMessage, cancellationToken);

        return new ForceExpireResult(monitoringExpired, jvExpired, replayExpired);
    }

    // Stuck panel shows Running jobs whose last activity is older than 2x the heartbeat window
    // (heartbeat = staleTimeout / 3). That picks up genuinely silent workers well before the
    // full stale-expiry threshold fires.
    private static TimeSpan ComputeActivityThreshold(int staleTimeoutSeconds)
    {
        var heartbeatSeconds = Math.Clamp(staleTimeoutSeconds / 3, 5, 30);
        return TimeSpan.FromSeconds(Math.Max(heartbeatSeconds * 2, 60));
    }

    private static TimeSpan ComputeReplayActivityThreshold(int staleTimeoutSeconds)
    {
        return TimeSpan.FromSeconds(Math.Max(staleTimeoutSeconds, 1));
    }

    private static TimeSpan ComputeQueueBacklogGracePeriod(int idleDelaySeconds)
    {
        return TimeSpan.FromSeconds(Math.Max(idleDelaySeconds * 3, 15));
    }

    private MonitoringProcessorHealthRow BuildMonitoringProcessorHealthRow(
        string category,
        IReadOnlyCollection<MonitoringJobRecord> activeJobs,
        IReadOnlySet<long> liveRuntimeJobIds,
        bool dmvAvailable,
        TimeSpan queueBacklogGracePeriod,
        DateTime nowUtc)
    {
        var configuredWorkers = _monitoringOptions.CategoryMaxConcurrentJobs.TryGetValue(category, out var categoryLimit)
            ? categoryLimit
            : _monitoringOptions.MaxConcurrentJobs;
        var queuedJobs = activeJobs
            .Where(job => MonitoringJobHelper.IsQueuedStatus(job.Status))
            .OrderBy(job => job.EnqueuedAt)
            .ToArray();
        var runningJobs = activeJobs.Where(job => MonitoringJobHelper.IsRunningStatus(job.Status)).ToArray();
        var liveRuntimeJobs = runningJobs.Count(job => liveRuntimeJobIds.Contains(job.JobId));
        var oldestQueuedAtUtc = queuedJobs.FirstOrDefault()?.EnqueuedAt;
        var desiredWorkers = Math.Min(configuredWorkers, queuedJobs.Length + liveRuntimeJobs);
        var queuedAgeExceeded = oldestQueuedAtUtc.HasValue && nowUtc - oldestQueuedAtUtc.Value >= queueBacklogGracePeriod;
        var hasIssue = dmvAvailable
            && queuedJobs.Length > 0
            && queuedAgeExceeded
            && liveRuntimeJobs < desiredWorkers;

        var status = dmvAvailable
            ? hasIssue
                ? "Underfilled"
                : queuedJobs.Length == 0 && runningJobs.Length == 0
                    ? "Idle"
                    : "Healthy"
            : "DMV unavailable";

        var detail = !dmvAvailable
            ? "Live worker verification is unavailable because the DMV runtime lookup failed."
            : hasIssue
                ? $"Queued work has waited past the grace period while live runtime is below the configured worker count ({liveRuntimeJobs}/{desiredWorkers})."
                : queuedJobs.Length == 0 && runningJobs.Length == 0
                    ? "No queued or running jobs for this processor."
                    : queuedJobs.Length > 0
                        ? $"Queued work is present, but the oldest queued row is still inside the {queueBacklogGracePeriod.TotalSeconds:0}-second grace period."
                        : "Live runtime matches the currently needed worker count.";

        return new MonitoringProcessorHealthRow(
            MonitoringJobHelper.GetCategoryDisplayName(category),
            category,
            configuredWorkers,
            queuedJobs.Length,
            runningJobs.Length,
            liveRuntimeJobs,
            oldestQueuedAtUtc,
            hasIssue,
            !dmvAvailable,
            status,
            detail);
    }
}
