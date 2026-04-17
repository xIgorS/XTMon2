namespace XTMon.Models;

public sealed record ReverseConsoFileResult(
    string ParsedQuery,
    MonitoringTableResult Table);