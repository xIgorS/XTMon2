using XTMon.Models;

namespace XTMon.Repositories;

public interface IMonitoringRepository
{
    Task<MonitoringTableResult> GetDbSizePlusDiskAsync(CancellationToken cancellationToken);
    Task<MonitoringTableResult> GetDbBackupsAsync(CancellationToken cancellationToken);
}
