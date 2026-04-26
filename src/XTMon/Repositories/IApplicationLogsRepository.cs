using XTMon.Models;

namespace XTMon.Repositories;

public interface IApplicationLogsRepository
{
    Task<IReadOnlyList<ApplicationLogRecord>> GetApplicationLogsAsync(
        ApplicationLogQuery query,
        CancellationToken cancellationToken);
}