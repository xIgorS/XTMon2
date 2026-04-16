using XTMon.Models;

namespace XTMon.Repositories;

public interface IColumnStoreCheckRepository
{
    Task<ColumnStoreCheckResult> GetColumnStoreCheckAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}