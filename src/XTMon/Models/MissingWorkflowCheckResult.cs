namespace XTMon.Models;

public sealed record MissingWorkflowCheckResult(
    string ParsedQuery,
    MonitoringTableResult Table);