using XTMon.Models;

namespace XTMon.Repositories;

public interface IPricingFileReceptionRepository
{
    Task<PricingFileReceptionResult> GetPricingFileReceptionAsync(DateOnly pnlDate, bool traceAllVersions, CancellationToken cancellationToken);
}
