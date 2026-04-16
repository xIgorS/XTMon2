namespace XTMon.Models;

public sealed record RolloveredPortfoliosResult(
    string ParsedQuery,
    MonitoringTableResult Table);