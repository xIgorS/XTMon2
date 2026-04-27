namespace XTMon.Models;

public sealed record MonitoringJobRecord(
    long JobId,
    string Category,
    string SubmenuKey,
    string? DisplayName,
    DateOnly PnlDate,
    string Status,
    string? WorkerId,
    string? ParametersJson,
    string? ParameterSummary,
    DateTime EnqueuedAt,
    DateTime? StartedAt,
    DateTime? LastHeartbeatAt,
    DateTime? CompletedAt,
    DateTime? FailedAt,
    string? ErrorMessage,
    string? ParsedQuery,
    string? GridColumnsJson,
    string? GridRowsJson,
    string? MetadataJson,
    DateTime? SavedAt,
    long? PersistedResultJobId = null);