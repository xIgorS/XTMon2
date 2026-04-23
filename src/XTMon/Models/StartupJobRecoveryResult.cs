namespace XTMon.Models;

public sealed record StartupJobRecoveryResult(int RecoveredMonitoringJobs, int RecoveredJvJobs)
{
    public int TotalRecoveredJobs => RecoveredMonitoringJobs + RecoveredJvJobs;
}