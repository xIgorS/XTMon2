using XTMon.Models;

namespace XTMon.Repositories;

public interface IResultTransferRepository
{
    Task<ResultTransferResult> GetResultTransferAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}