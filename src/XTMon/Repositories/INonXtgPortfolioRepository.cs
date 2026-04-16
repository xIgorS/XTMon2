using XTMon.Models;

namespace XTMon.Repositories;

public interface INonXtgPortfolioRepository
{
    Task<NonXtgPortfolioResult> GetNonXtgPortfolioAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}