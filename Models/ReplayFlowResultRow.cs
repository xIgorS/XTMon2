namespace XTMon.Models;

public sealed record ReplayFlowResultRow(
    long FlowIdDerivedFrom,
    long FlowId,
    DateOnly PnlDate,
    Guid PackageGuid,
    bool WithBackdated,
    bool SkipCoreProcess,
    bool DropTableTmp,
    DateTime DateCreated,
    string CreatedBy);
