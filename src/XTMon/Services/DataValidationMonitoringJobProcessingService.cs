using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Options;

namespace XTMon.Services;

public sealed class DataValidationMonitoringJobProcessingService : MonitoringJobProcessingService
{
    public DataValidationMonitoringJobProcessingService(
        IServiceScopeFactory scopeFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<DataValidationMonitoringJobProcessingService> logger,
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

    internal DataValidationMonitoringJobProcessingService(
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
            processorName: "Data Validation monitoring job processing service",
            ownedCategories: [MonitoringJobHelper.DataValidationCategory],
            excludedSubmenuKeys: [MonitoringJobHelper.DailyBalanceSubmenuKey, MonitoringJobHelper.PricingSubmenuKey],
            maxConcurrentJobs: GetConfiguredWorkerCount(options.Value, MonitoringJobHelper.DataValidationCategory))
    {
    }

    private static int GetConfiguredWorkerCount(MonitoringJobsOptions options, string category)
    {
        return options.CategoryMaxConcurrentJobs.TryGetValue(category, out var configuredWorkers)
            ? configuredWorkers
            : options.MaxConcurrentJobs;
    }
}