using XTMon.Models;

namespace XTMon.Repositories;

public interface IPrecalcMonitoringRepository
{
    Task<PrecalcMonitoringResult> GetPrecalcMonitoringAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}