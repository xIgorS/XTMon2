using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class JvCalculationOptions
{
    public const string SectionName = "JvCalculation";

    [Required]
    public string PnlDatesConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string PublicationConnectionStringName { get; set; } = "Publication";

    [Required]
    public string GetPnlDatesStoredProcedure { get; set; } = "replay.UspGetPnlDates";

    [Required]
    public string CheckJvCalculationStoredProcedure { get; set; } = "monitoring.UspCheckJvCalculation";

    [Required]
    public string FixJvCalculationStoredProcedure { get; set; } = "Administration.UspCleanUpJvissue";

    [Required]
    public string JobConnectionStringName { get; set; } = "LogFiAlmt";

    [Required]
    public string JobEnqueueStoredProcedure { get; set; } = "monitoring.UspJvJobEnqueue";

    [Required]
    public string JobTakeNextStoredProcedure { get; set; } = "monitoring.UspJvJobTakeNext";

    [Required]
    public string JobHeartbeatStoredProcedure { get; set; } = "monitoring.UspJvJobHeartbeat";

    [Required]
    public string JobSaveResultStoredProcedure { get; set; } = "monitoring.UspJvJobSaveResult";

    [Required]
    public string JobMarkCompletedStoredProcedure { get; set; } = "monitoring.UspJvJobMarkCompleted";

    [Required]
    public string JobMarkFailedStoredProcedure { get; set; } = "monitoring.UspJvJobMarkFailed";

    [Required]
    public string JobMarkCancelledStoredProcedure { get; set; } = "monitoring.UspJvJobMarkCancelled";

    [Required]
    public string JobCancelActiveStoredProcedure { get; set; } = "monitoring.UspJvJobCancelActive";

    [Required]
    public string JobCountActiveStoredProcedure { get; set; } = "monitoring.UspJvJobCountActive";

    [Required]
    public string JobGetStuckStoredProcedure { get; set; } = "monitoring.UspJvJobGetStuck";

    [Required]
    public string JobGetByIdStoredProcedure { get; set; } = "monitoring.UspJvJobGetById";

    [Required]
    public string JobGetLatestStoredProcedure { get; set; } = "monitoring.UspJvJobGetLatestByUserPnlDate";

    [Required]
    public string JobExpireStaleStoredProcedure { get; set; } = "monitoring.UspJvJobExpireStale";

    [Required]
    public string JobFailRunningStoredProcedure { get; set; } = "monitoring.UspJvJobFailRunning";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    [Range(0d, 99.99d)]
    public decimal Precision { get; set; } = 0.01m;

    [Range(1, 3600)]
    public int JobPollIntervalSeconds { get; set; } = 5;

    [Range(30, 86400)]
    public int JobRunningStaleTimeoutSeconds { get; set; } = 900;

    [Range(1, 300)]
    public int ProcessorIdleDelaySeconds { get; set; } = 5;

    [Range(1, 300)]
    public int ProcessorMarkStateShutdownGraceSeconds { get; set; } = 10;

    [Range(1, 60)]
    public int ProcessorMarkStateRetryDelaySeconds { get; set; } = 2;
}
