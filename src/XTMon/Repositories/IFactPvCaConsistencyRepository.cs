using XTMon.Models;

namespace XTMon.Repositories;

public interface IFactPvCaConsistencyRepository
{
    Task<FactPvCaConsistencyResult> GetFactPvCaConsistencyAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}