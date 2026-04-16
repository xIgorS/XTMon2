using XTMon.Models;

namespace XTMon.Repositories;

public interface IOutOfScopePortfolioRepository
{
    Task<OutOfScopePortfolioResult> GetOutOfScopePortfolioAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}