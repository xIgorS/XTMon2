namespace XTMon.Models;

public sealed record AdjustmentsResult(
    string ParsedQuery,
    MonitoringTableResult Table);
