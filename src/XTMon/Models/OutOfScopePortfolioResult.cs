namespace XTMon.Models;

public sealed record OutOfScopePortfolioResult(
    string ParsedQuery,
    MonitoringTableResult Table);