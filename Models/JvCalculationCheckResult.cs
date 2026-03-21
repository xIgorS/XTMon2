namespace XTMon.Models;

public sealed record JvCalculationCheckResult(
    string ParsedQuery,
    MonitoringTableResult Table);
