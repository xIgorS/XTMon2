using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Options;

namespace XTMon.Services;

public sealed class DailyBalanceMonitoringJobProcessingService : MonitoringJobProcessingService
{
    public DailyBalanceMonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<DailyBalanceMonitoringJobProcessingService> logger,
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

    internal DailyBalanceMonitoringJobProcessingService(
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
            processorName: "Daily Balance monitoring job processing service",
            ownedCategories: [MonitoringJobHelper.DataValidationCategory],
            includedSubmenuKeys: [MonitoringJobHelper.DailyBalanceSubmenuKey],
            maxConcurrentJobs: 1)
    {
    }
}