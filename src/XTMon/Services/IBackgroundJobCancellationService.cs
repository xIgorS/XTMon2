namespace XTMon.Services;

public interface IBackgroundJobCancellationService
{
    Task<BackgroundJobCancellationResult> CancelMonitoringJobAsync(long jobId, CancellationToken cancellationToken);
    Task<BackgroundJobCancellationResult> CancelJvJobAsync(long jobId, CancellationToken cancellationToken);
    Task<BackgroundJobBulkCancellationResult> CancelAllBackgroundJobsAsync(CancellationToken cancellationToken);
}