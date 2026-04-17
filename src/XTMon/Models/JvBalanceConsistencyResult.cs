namespace XTMon.Models;

public sealed record JvBalanceConsistencyResult(
    string ParsedQuery,
    MonitoringTableResult Table);