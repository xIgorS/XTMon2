namespace XTMon.Models;

public sealed record MarketDataResult(
    string ParsedQuery,
    MonitoringTableResult Table);