namespace XTMon.Models;

public sealed record SasTablesResult(
    string ParsedQuery,
    MonitoringTableResult Table);