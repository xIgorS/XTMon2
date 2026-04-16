namespace XTMon.Models;

public sealed record MirrorizationResult(
    string ParsedQuery,
    MonitoringTableResult Table);