using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MonitoringJobsOptions
{
    public const string SectionName = "MonitoringJobs";

    [Required]
    public string JobConnectionStringName { get; set; } = "LogFiAlmt";

    [Required]
    public string JobEnqueueStoredProcedure { get; set; } = "monitoring.UspMonitoringJobEnqueue";

    [Required]
    public string JobTakeNextStoredProcedure { get; set; } = "monitoring.UspMonitoringJobTakeNext";

    [Required]
    public string JobHeartbeatStoredProcedure { get; set; } = "monitoring.UspMonitoringJobHeartbeat";

    [Required]
    public string JobSaveResultStoredProcedure { get; set; } = "monitoring.UspMonitoringJobSaveResult";

    [Required]
    public string JobMarkCompletedStoredProcedure { get; set; } = "monitoring.UspMonitoringJobMarkCompleted";

    [Required]
    public string JobMarkFailedStoredProcedure { get; set; } = "monitoring.UspMonitoringJobMarkFailed";

    [Required]
    public string JobGetByIdStoredProcedure { get; set; } = "monitoring.UspMonitoringJobGetById";

    [Required]
    public string JobGetLatestStoredProcedure { get; set; } = "monitoring.UspMonitoringJobGetLatestByKey";

    [Required]
    public string JobGetLatestByCategoryStoredProcedure { get; set; } = "monitoring.UspMonitoringJobGetLatestByCategory";

    [Required]
    public string JobExpireStaleStoredProcedure { get; set; } = "monitoring.UspMonitoringJobExpireStale";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    [Range(1, 3600)]
    public int JobPollIntervalSeconds { get; set; } = 5;

    [Range(1, 3600)]
    public int NavAlertPollIntervalSeconds { get; set; } = 30;

    [Range(1, 16)]
    public int MaxConcurrentJobs { get; set; } = 1;

    public Dictionary<string, int> CategoryMaxConcurrentJobs { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [Range(30, 86400)]
    public int JobRunningStaleTimeoutSeconds { get; set; } = 900;

    [Range(1, 300)]
    public int ProcessorIdleDelaySeconds { get; set; } = 5;

    [Range(1, 300)]
    public int ProcessorMarkStateShutdownGraceSeconds { get; set; } = 10;

    [Range(1, 60)]
    public int ProcessorMarkStateRetryDelaySeconds { get; set; } = 2;
}