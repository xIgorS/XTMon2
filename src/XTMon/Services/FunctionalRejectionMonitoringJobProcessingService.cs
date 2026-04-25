using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Options;

namespace XTMon.Services;

public sealed class FunctionalRejectionMonitoringJobProcessingService : MonitoringJobProcessingService
{
    public FunctionalRejectionMonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<FunctionalRejectionMonitoringJobProcessingService> logger,
        JobCancellationRegistry jobCancellationRegistry)
        : this(
            scopeFactory,
            options,
            logger,
            TimeSpan.FromSeconds(options.Value.ProcessorIdleDelaySeconds),
            null,
            jobCancellationRegistry)
    {
    }

    internal FunctionalRejectionMonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger logger,
        TimeSpan idleDelay,
        TimeSpan? heartbeatInterval = null,
        JobCancellationRegistry? jobCancellationRegistry = null)
        : base(
            scopeFactory,
            options,
            logger,
            idleDelay,
            heartbeatInterval,
            jobCancellationRegistry,
            processorName: "Functional Rejection monitoring job processing service",
            ownedCategories: [MonitoringJobHelper.FunctionalRejectionCategory],
            maxConcurrentJobs: GetConfiguredWorkerCount(options.Value, MonitoringJobHelper.FunctionalRejectionCategory))
    {
    }

    private static int GetConfiguredWorkerCount(MonitoringJobsOptions options, string category)
    {
        return options.CategoryMaxConcurrentJobs.TryGetValue(category, out var configuredWorkers)
            ? configuredWorkers
            : options.MaxConcurrentJobs;
    }
}