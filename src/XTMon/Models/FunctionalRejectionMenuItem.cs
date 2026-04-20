namespace XTMon.Models;

public sealed record FunctionalRejectionMenuItem(
    string SourceSystemBusinessDataTypeCode,
    int BusinessDataTypeId,
    string SourceSystemName,
    string DbConnection);