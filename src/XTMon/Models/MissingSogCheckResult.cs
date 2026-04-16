namespace XTMon.Models;

public sealed record MissingSogCheckResult(
    string ParsedQuery,
    MonitoringTableResult Table);