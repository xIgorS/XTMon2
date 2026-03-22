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

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    [Range(1, 7200)]
    public int ProcessCommandTimeoutSeconds { get; set; } = 3600;

    [Range(1, 3600)]
    public int StatusPollIntervalSeconds { get; set; } = 15;
}
