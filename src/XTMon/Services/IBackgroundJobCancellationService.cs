namespace XTMon.Services;

public interface IBackgroundJobCancellationService
{
    Task<bool> CancelMonitoringJobAsync(long jobId, CancellationToken cancellationToken);
    Task<bool> CancelJvJobAsync(long jobId, CancellationToken cancellationToken);
}