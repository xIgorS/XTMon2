using XTMon.Models;

namespace XTMon.Repositories;

public interface ITradingVsFivrCheckRepository
{
    Task<TradingVsFivrCheckResult> GetTradingVsFivrCheckAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}