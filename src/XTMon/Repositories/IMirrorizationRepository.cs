using XTMon.Models;

namespace XTMon.Repositories;

public interface IMirrorizationRepository
{
    Task<MirrorizationResult> GetMirrorizationAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}