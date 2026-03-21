namespace XTMon.Models;

public sealed record ReplayFlowStatusRow(
    long FlowId,
    long FlowIdDerivedFrom,
    DateOnly PnlDate,
    Guid PackageGuid,
    bool WithBackdated,
    bool SkipCoreProcess,
    bool DropTableTmp,
    DateTime DateCreated,
    string CreatedBy,
    DateTime? DateSubmitted,
    DateTime? DateStarted,
    DateTime? DateCompleted,
    string? Status,
    string? ProcessStatus,
    string? Duration);
