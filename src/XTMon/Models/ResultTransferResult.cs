namespace XTMon.Models;

public sealed record ResultTransferResult(
    string ParsedQuery,
    MonitoringTableResult Table);