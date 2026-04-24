namespace XTMon.Models;

public sealed record ForceExpireResult(int MonitoringJobsExpired, int JvJobsExpired, int ReplayBatchesExpired)
{
    public int TotalExpired => MonitoringJobsExpired + JvJobsExpired + ReplayBatchesExpired;
}
