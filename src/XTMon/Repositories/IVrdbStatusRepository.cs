using XTMon.Models;

namespace XTMon.Repositories;

public interface IVrdbStatusRepository
{
    Task<VrdbStatusResult> GetVrdbStatusAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}