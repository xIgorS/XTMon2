using XTMon.Models;

namespace XTMon.Repositories;

public interface ISystemDiagnosticsRepository
{
    Task<int> CleanLoggingAsync(CancellationToken cancellationToken);

    Task<SystemDiagnosticsHistoryCleanupResult> CleanHistoryAsync(CancellationToken cancellationToken);
}