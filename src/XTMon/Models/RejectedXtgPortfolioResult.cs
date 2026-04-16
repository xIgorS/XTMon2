namespace XTMon.Models;

public sealed record RejectedXtgPortfolioResult(
    string ParsedQuery,
    MonitoringTableResult Table);