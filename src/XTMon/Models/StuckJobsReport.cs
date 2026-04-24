namespace XTMon.Models;

public sealed record StuckJobsReport(
    IReadOnlyList<MonitoringJobRecord> StuckMonitoringJobs,
    IReadOnlyList<JvJobRecord> StuckJvJobs,
    IReadOnlyList<StuckReplayBatchRow> StuckReplayBatches,
    DateTime GeneratedAtUtc)
{
    public int TotalStuckCount =>
        StuckMonitoringJobs.Count + StuckJvJobs.Count + StuckReplayBatches.Count;
}
