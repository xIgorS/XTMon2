namespace XTMon.Models;

public sealed record ColumnStoreCheckResult(
    string ParsedQuery,
    MonitoringTableResult Table);