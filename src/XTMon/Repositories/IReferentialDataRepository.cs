using XTMon.Models;

namespace XTMon.Repositories;

public interface IReferentialDataRepository
{
    Task<ReferentialDataResult> GetReferentialDataAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}