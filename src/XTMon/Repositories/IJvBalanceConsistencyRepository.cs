using XTMon.Models;

namespace XTMon.Repositories;

public interface IJvBalanceConsistencyRepository
{
    Task<JvBalanceConsistencyResult> GetJvBalanceConsistencyAsync(DateOnly pnlDate, decimal precision, CancellationToken cancellationToken);
}