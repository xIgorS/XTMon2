namespace XTMon.Services;

public readonly record struct BackgroundJobBulkCancellationResult(
    int MonitoringJobsCancelled,
    int JvJobsCancelled,
    int MonitoringWorkersCancellationRequested,
    int JvWorkersCancellationRequested,
    int ActiveMonitoringJobsRemaining,
    int ActiveJvJobsRemaining)
{
    public int TotalJobsCancelled => MonitoringJobsCancelled + JvJobsCancelled;

    public int TotalWorkersCancellationRequested => MonitoringWorkersCancellationRequested + JvWorkersCancellationRequested;

    public int TotalActiveJobsRemaining => ActiveMonitoringJobsRemaining + ActiveJvJobsRemaining;

    public bool CancellationConfirmed => TotalActiveJobsRemaining == 0;
}