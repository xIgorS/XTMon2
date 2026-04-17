using XTMon.Models;

namespace XTMon.Repositories;

public interface IReverseConsoFileRepository
{
    Task<IReadOnlyList<ReverseConsoFileSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken);

    Task<ReverseConsoFileResult> GetReverseConsoFileAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken);
}