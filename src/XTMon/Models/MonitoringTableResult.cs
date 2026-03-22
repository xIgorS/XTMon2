namespace XTMon.Models;

public sealed record MonitoringTableResult(
    IReadOnlyList<string> Columns,
    IReadOnlyList<IReadOnlyList<string?>> Rows);
