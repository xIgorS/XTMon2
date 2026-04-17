namespace XTMon.Models;

public sealed record VrdbStatusResult(
    string ParsedQuery,
    MonitoringTableResult Table);