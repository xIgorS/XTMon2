namespace XTMon.Models;

public sealed record FutureCashResult(
    string ParsedQuery,
    MonitoringTableResult Table);