namespace XTMon.Models;

public sealed record FeedOutExtractionResult(
    string ParsedQuery,
    MonitoringTableResult Table);