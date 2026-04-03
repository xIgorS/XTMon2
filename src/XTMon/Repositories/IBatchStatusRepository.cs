using XTMon.Models;

namespace XTMon.Repositories;

public interface IBatchStatusRepository
{
    Task<MonitoringTableResult> GetBatchStatusAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}