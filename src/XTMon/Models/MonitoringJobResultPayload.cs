namespace XTMon.Models;

public sealed record MonitoringJobResultPayload(
    string? ParsedQuery,
    MonitoringTableResult? Table,
    string? MetadataJson);