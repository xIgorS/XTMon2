using XTMon.Models;

namespace XTMon.Repositories;

public interface IMarketDataRepository
{
    Task<MarketDataResult> GetMarketDataAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}