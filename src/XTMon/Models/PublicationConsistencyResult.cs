namespace XTMon.Models;

public sealed record PublicationConsistencyResult(
    string ParsedQuery,
    MonitoringTableResult Table);