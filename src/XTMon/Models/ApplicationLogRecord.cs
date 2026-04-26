namespace XTMon.Models;

public sealed record ApplicationLogRecord(
    int Id,
    DateTime TimeStamp,
    string Level,
    string? Message,
    string? Exception,
    string? Properties);

public sealed record ApplicationLogQuery(
    int TopN,
    DateTime? FromTimeStamp,
    DateTime? ToTimeStamp,
    IReadOnlyCollection<string> Levels,
    string? MessageContains);