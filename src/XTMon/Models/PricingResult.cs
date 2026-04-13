namespace XTMon.Models;

public sealed record PricingResult(
    string ParsedQuery,
    MonitoringTableResult Table);