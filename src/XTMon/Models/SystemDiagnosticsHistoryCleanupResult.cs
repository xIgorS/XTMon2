namespace XTMon.Models;

public readonly record struct SystemDiagnosticsHistoryCleanupResult(
    int MonitoringLatestResultsDeleted,
    int MonitoringJobsDeleted,
    int JvCalculationJobResultsDeleted,
    int JvCalculationJobsDeleted)
{
    public int TotalDeleted =>
        MonitoringLatestResultsDeleted +
        MonitoringJobsDeleted +
        JvCalculationJobResultsDeleted +
        JvCalculationJobsDeleted;
}