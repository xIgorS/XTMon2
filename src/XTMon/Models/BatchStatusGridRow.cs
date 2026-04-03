namespace XTMon.Models;

public sealed record BatchStatusGridRow(
    string Status,
    string PnlDate,
    string CalculationDate,
    string CalculationEndTime,
    string ExtractionEndTime);