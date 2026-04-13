namespace XTMon.Models;

public sealed record DailyBalanceResult(
    string ParsedQuery,
    MonitoringTableResult Table);
