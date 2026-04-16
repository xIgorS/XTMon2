using XTMon.Models;

namespace XTMon.Repositories;

public interface IAdjustmentLinksCheckRepository
{
    Task<AdjustmentLinksCheckResult> GetAdjustmentLinksCheckAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}