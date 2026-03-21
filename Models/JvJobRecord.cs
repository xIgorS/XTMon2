namespace XTMon.Models;

public sealed record JvJobRecord(
    long JobId,
    string UserId,
    DateOnly PnlDate,
    string RequestType,
    string Status,
    string? WorkerId,
    DateTime EnqueuedAt,
    DateTime? StartedAt,
    DateTime? LastHeartbeatAt,
    DateTime? CompletedAt,
    DateTime? FailedAt,
    string? ErrorMessage,
    string? QueryCheck,
    string? QueryFix,
    string? GridColumnsJson,
    string? GridRowsJson,
    DateTime? SavedAt);
