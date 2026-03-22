namespace XTMon.Models;

public sealed record FailedFlowRow(
    long? FlowId,
    long? FlowIdDerivedFrom,
    string? BusinessDataType,
    string? FeedSource,
    DateOnly PnlDate,
    Guid PackageGuid,
    string? FileName,
    DateTime? ArrivalDate,
    string? CurrentStep,
    bool IsFailed,
    string? TypeOfCalculation,
    bool IsAdjustment,
    bool IsReplay,
    bool WithBackdated,
    bool SkipCoreProcess,
    bool DropTableTmp);
