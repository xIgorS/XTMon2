namespace XTMon.Models;

public sealed record ReferentialDataResult(
    string ParsedQuery,
    MonitoringTableResult Table);