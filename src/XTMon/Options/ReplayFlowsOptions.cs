using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class ReplayFlowsOptions
{
    public const string SectionName = "ReplayFlows";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string GetFailedFlowsStoredProcedure { get; set; } = "Replay.UspGetFailedFlows";

    [Required]
    public string ReplayFlowsStoredProcedure { get; set; } = "Replay.UspInsertReplayFlows";

    [Required]
    public string ReplayFlowsTableTypeName { get; set; } = "Replay.ReplayAdjAtCoreSet";

    [Required]
    public string ProcessReplayFlowsStoredProcedure { get; set; } = "Replay.UspProcessReplayFlows";

    [Required]
    public string GetReplayFlowStatusStoredProcedure { get; set; } = "Replay.UspGetReplayFlowStatus";

    [Required]
    public string GetReplayFlowProcessStatusStoredProcedure { get; set; } = "Replay.UspGetReplayFlowProcessStatus";

    // Recovery SPs live in LOG_FI_ALMT (where administration.ReplayFlows lives) to avoid
    // cross-database UPDATEs. The primary ConnectionStringName above points to STAGING_FI_ALMT
    // for the existing Replay.* SPs, so these need their own connection name.
    [Required]
    public string RecoveryConnectionStringName { get; set; } = "LogFiAlmt";

    [Required]
    public string FailStaleReplayBatchesStoredProcedure { get; set; } = "administration.UspFailStaleReplayBatches";

    [Required]
    public string FailRunningReplayBatchesStoredProcedure { get; set; } = "administration.UspFailRunningReplayBatches";

    [Required]
    public string GetStuckReplayBatchesStoredProcedure { get; set; } = "administration.UspGetStuckReplayBatches";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    [Range(1, 7200)]
    public int ProcessCommandTimeoutSeconds { get; set; } = 3600;

    [Range(1, 3600)]
    public int StatusPollIntervalSeconds { get; set; } = 15;

    [Range(1, 7200)]
    public int RunningStaleTimeoutSeconds { get; set; } = 900;
}
