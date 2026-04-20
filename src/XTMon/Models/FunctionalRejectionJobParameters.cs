namespace XTMon.Models;

public sealed record FunctionalRejectionJobParameters(
    string? SourceSystemBusinessDataTypeCode,
    int BusinessDataTypeId,
    string SourceSystemName,
    string DbConnection);