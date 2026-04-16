namespace XTMon.Models;

public sealed record BatchStatusGridRow(
    string Status,
    string PnlDate,
    string SourceSystemName,
    string CalculationDate,
    string CalculationEndTime,
    string ExtractionEndTime);