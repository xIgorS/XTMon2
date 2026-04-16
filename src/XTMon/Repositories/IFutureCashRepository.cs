using XTMon.Models;

namespace XTMon.Repositories;

public interface IFutureCashRepository
{
    Task<FutureCashResult> GetFutureCashAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}