using XTMon.Models;

namespace XTMon.Repositories;

public interface IDailyBalanceRepository
{
    Task<IReadOnlyList<DailyBalanceSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken);

    Task<DailyBalanceResult> GetDailyBalanceAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken);
}
