namespace XTMon.Models;

public sealed record StuckReplayBatchRow(
    long FlowId,
    long FlowIdDerivedFrom,
    DateOnly PnlDate,
    Guid PackageGuid,
    string? CreatedBy,
    DateTime DateCreated,
    DateTime? DateStarted,
    DateTime? DateCompleted,
    string? ReplayStatus,
    string? ProcessStatus,
    int AgeSeconds);
