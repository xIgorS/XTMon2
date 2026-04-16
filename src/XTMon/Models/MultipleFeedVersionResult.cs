namespace XTMon.Models;

public sealed record MultipleFeedVersionResult(
    string ParsedQuery,
    MonitoringTableResult Table);