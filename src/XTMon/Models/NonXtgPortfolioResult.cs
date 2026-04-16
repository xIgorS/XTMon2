namespace XTMon.Models;

public sealed record NonXtgPortfolioResult(
    string ParsedQuery,
    MonitoringTableResult Table);