using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Options;

namespace XTMon.Services;

public sealed class PricingMonitoringJobProcessingService : MonitoringJobProcessingService
{
    public PricingMonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<PricingMonitoringJobProcessingService> logger,
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

    internal PricingMonitoringJobProcessingService(
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
            processorName: "Pricing monitoring job processing service",
            ownedCategories: [MonitoringJobHelper.DataValidationCategory],
            includedSubmenuKeys: [MonitoringJobHelper.PricingSubmenuKey],
            maxConcurrentJobs: 1)
    {
    }
}