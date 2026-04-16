using XTMon.Models;

namespace XTMon.Repositories;

public interface IMissingSogCheckRepository
{
    Task<MissingSogCheckResult> GetMissingSogCheckAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}