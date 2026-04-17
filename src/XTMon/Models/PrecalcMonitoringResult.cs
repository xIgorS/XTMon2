namespace XTMon.Models;

public sealed record PrecalcMonitoringResult(
    string ParsedQuery,
    MonitoringTableResult Table);