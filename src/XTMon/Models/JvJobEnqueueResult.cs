namespace XTMon.Models;

public sealed record JvJobEnqueueResult(
    long JobId,
    bool AlreadyActive);
