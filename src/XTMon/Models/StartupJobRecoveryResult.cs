namespace XTMon.Models;

public sealed record StartupJobRecoveryResult(int RecoveredMonitoringJobs, int RecoveredJvJobs, int RecoveredReplayBatches)
{
    public int TotalRecoveredJobs => RecoveredMonitoringJobs + RecoveredJvJobs + RecoveredReplayBatches;
}