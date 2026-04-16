namespace XTMon.Models;

public sealed record TradingVsFivrCheckResult(
    string ParsedQuery,
    MonitoringTableResult Table);