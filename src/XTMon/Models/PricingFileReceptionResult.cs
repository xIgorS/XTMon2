namespace XTMon.Models;

public sealed record PricingFileReceptionResult(
    string ParsedQuery,
    MonitoringTableResult Table);
