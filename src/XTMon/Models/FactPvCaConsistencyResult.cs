namespace XTMon.Models;

public sealed record FactPvCaConsistencyResult(
    string ParsedQuery,
    MonitoringTableResult Table);