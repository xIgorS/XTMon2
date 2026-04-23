using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class StartupJobRecoveryService : IHostedService
{
    internal const string MonitoringStartupRecoveryMessage = "Monitoring background job was still marked Running when the application started and was failed during startup recovery.";
    internal const string JvStartupRecoveryMessage = "JV background job was still marked Running when the application started and was failed during startup recovery.";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<StartupJobRecoveryService> _logger;

    public StartupJobRecoveryService(
        IServiceScopeFactory scopeFactory,
        ILogger<StartupJobRecoveryService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            var result = await RecoverAsync(cancellationToken);

            if (result.TotalRecoveredJobs == 0)
            {
                _logger.LogInformation("Startup job recovery found no orphaned running jobs.");
                return;
            }

            _logger.LogWarning(
                "Startup job recovery failed {RecoveredMonitoringJobs} monitoring job(s) and {RecoveredJvJobs} JV job(s) left in Running state by an earlier application instance.",
                result.RecoveredMonitoringJobs,
                result.RecoveredJvJobs);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Startup job recovery failed. The application will continue starting, but orphaned running jobs may remain blocked until regular stale-job expiry runs.");
        }
    }

    public async Task<StartupJobRecoveryResult> RecoverAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var monitoringRepository = scope.ServiceProvider.GetRequiredService<IMonitoringJobRepository>();
        var jvRepository = scope.ServiceProvider.GetRequiredService<IJvCalculationRepository>();

        var recoveredMonitoringJobs = await monitoringRepository.FailRunningMonitoringJobsAsync(MonitoringStartupRecoveryMessage, cancellationToken);
        var recoveredJvJobs = await jvRepository.FailRunningJvJobsAsync(JvStartupRecoveryMessage, cancellationToken);

        return new StartupJobRecoveryResult(recoveredMonitoringJobs, recoveredJvJobs);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}