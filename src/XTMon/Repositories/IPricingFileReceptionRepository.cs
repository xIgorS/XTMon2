using XTMon.Models;

namespace XTMon.Repositories;

public interface IPricingFileReceptionRepository
{
    Task<IReadOnlyList<PricingFileReceptionSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken);

    Task<PricingFileReceptionResult> GetPricingFileReceptionAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken);
}
