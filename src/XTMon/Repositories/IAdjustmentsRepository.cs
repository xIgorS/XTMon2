using XTMon.Models;

namespace XTMon.Repositories;

public interface IAdjustmentsRepository
{
    Task<IReadOnlyList<AdjustmentsSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken);

    Task<AdjustmentsResult> GetAdjustmentsAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken);
}
