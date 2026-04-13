using XTMon.Models;

namespace XTMon.Repositories;

public interface IPricingRepository
{
    Task<IReadOnlyList<PricingSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken);

    Task<PricingResult> GetPricingAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken);
}