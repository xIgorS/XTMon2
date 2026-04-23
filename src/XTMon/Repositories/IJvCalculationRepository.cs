using XTMon.Models;

namespace XTMon.Repositories;

public interface IJvCalculationRepository
{
    Task<JvPnlDatesResult> GetJvPnlDatesAsync(CancellationToken cancellationToken);
    Task<JvCalculationCheckResult> CheckJvCalculationAsync(DateOnly pnlDate, CancellationToken cancellationToken);
    Task<string> FixJvCalculationAsync(DateOnly pnlDate, bool executeCatchup, CancellationToken cancellationToken);
    Task<JvJobEnqueueResult> EnqueueJvJobAsync(string userId, DateOnly pnlDate, string requestType, CancellationToken cancellationToken);
    Task<JvJobRecord?> TryTakeNextJvJobAsync(string workerId, CancellationToken cancellationToken);
    Task<JvJobRecord?> GetJvJobByIdAsync(long jobId, CancellationToken cancellationToken);
    Task<JvJobRecord?> GetLatestJvJobAsync(string userId, DateOnly pnlDate, string? requestType, CancellationToken cancellationToken);
    Task SaveJvJobResultAsync(long jobId, string? queryCheck, string? queryFix, MonitoringTableResult? table, CancellationToken cancellationToken);
    Task MarkJvJobCompletedAsync(long jobId, CancellationToken cancellationToken);
    Task MarkJvJobFailedAsync(long jobId, string errorMessage, CancellationToken cancellationToken);
    Task HeartbeatJvJobAsync(long jobId, CancellationToken cancellationToken);
    Task<int> FailRunningJvJobsAsync(string errorMessage, CancellationToken cancellationToken);
    Task<int> ExpireStaleRunningJobsAsync(TimeSpan staleAfter, string errorMessage, CancellationToken cancellationToken);
}
