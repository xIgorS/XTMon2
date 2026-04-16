namespace XTMon.Models;

public sealed record AdjustmentLinksCheckResult(
    string ParsedQuery,
    MonitoringTableResult Table);