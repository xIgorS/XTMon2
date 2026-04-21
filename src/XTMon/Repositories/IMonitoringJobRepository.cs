using XTMon.Models;

namespace XTMon.Repositories;

public interface IMonitoringJobRepository
{
    Task<MonitoringJobEnqueueResult> EnqueueMonitoringJobAsync(
        string category,
        string submenuKey,
        string? displayName,
        DateOnly pnlDate,
        string? parametersJson,
        string? parameterSummary,
        CancellationToken cancellationToken);

    Task<MonitoringJobRecord?> TryTakeNextMonitoringJobAsync(string workerId, CancellationToken cancellationToken);
    Task<MonitoringJobRecord?> GetMonitoringJobByIdAsync(long jobId, CancellationToken cancellationToken);
    Task<MonitoringJobRecord?> GetLatestMonitoringJobAsync(string category, string submenuKey, DateOnly pnlDate, CancellationToken cancellationToken);
    Task<IReadOnlyList<MonitoringJobRecord>> GetLatestMonitoringJobsByCategoryAsync(string category, DateOnly pnlDate, CancellationToken cancellationToken);
    Task SaveMonitoringJobResultAsync(long jobId, MonitoringJobResultPayload payload, CancellationToken cancellationToken);
    Task MarkMonitoringJobCompletedAsync(long jobId, CancellationToken cancellationToken);
    Task MarkMonitoringJobFailedAsync(long jobId, string errorMessage, CancellationToken cancellationToken);
    Task HeartbeatMonitoringJobAsync(long jobId, CancellationToken cancellationToken);
    Task<int> ExpireStaleRunningMonitoringJobsAsync(TimeSpan staleAfter, string errorMessage, CancellationToken cancellationToken);
}