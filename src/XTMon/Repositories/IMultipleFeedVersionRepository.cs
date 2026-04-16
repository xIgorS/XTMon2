using XTMon.Models;

namespace XTMon.Repositories;

public interface IMultipleFeedVersionRepository
{
    Task<MultipleFeedVersionResult> GetMultipleFeedVersionAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}