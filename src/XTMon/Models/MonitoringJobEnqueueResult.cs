namespace XTMon.Models;

public sealed record MonitoringJobEnqueueResult(
    long JobId,
    bool AlreadyActive);