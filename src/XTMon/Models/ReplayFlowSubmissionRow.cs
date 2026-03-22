namespace XTMon.Models;

public sealed record ReplayFlowSubmissionRow(
    long FlowIdDerivedFrom,
    long FlowId,
    DateOnly PnlDate,
    Guid PackageGuid,
    bool WithBackdated,
    bool SkipCoreProcess,
    bool DropTableTmp);
