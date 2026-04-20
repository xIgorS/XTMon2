using XTMon.Models;

namespace XTMon.Services;

public interface IMonitoringJobExecutor
{
    bool CanExecute(MonitoringJobRecord job);

    Task<MonitoringJobResultPayload> ExecuteAsync(MonitoringJobRecord job, CancellationToken cancellationToken);
}