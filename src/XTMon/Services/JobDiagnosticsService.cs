using Microsoft.Extensions.Options;
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
    private readonly MonitoringJobsOptions _monitoringOptions;
    private readonly JvCalculationOptions _jvOptions;
    private readonly ReplayFlowsOptions _replayOptions;

    public JobDiagnosticsService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> monitoringOptions,
        IOptions<JvCalculationOptions> jvOptions,
        IOptions<ReplayFlowsOptions> replayOptions)
    {
        _scopeFactory = scopeFactory;
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

        return new StuckJobsReport(monitoringStuck, jvStuck, replayStuck, DateTime.UtcNow);
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

        var monitoringExpired = await monitoringRepository.ExpireStaleRunningMonitoringJobsAsync(monitoringThreshold, MonitoringForceExpireMessage, cancellationToken);
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
}
