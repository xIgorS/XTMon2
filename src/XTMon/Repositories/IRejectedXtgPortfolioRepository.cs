using XTMon.Models;

namespace XTMon.Repositories;

public interface IRejectedXtgPortfolioRepository
{
    Task<RejectedXtgPortfolioResult> GetRejectedXtgPortfolioAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}