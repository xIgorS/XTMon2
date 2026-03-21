using XTMon.Models;

namespace XTMon.Data;

public interface IMonitoringRepository
{
    Task<MonitoringTableResult> GetDbSizePlusDiskAsync(CancellationToken cancellationToken);
    Task<MonitoringTableResult> GetDbBackupsAsync(CancellationToken cancellationToken);
}
