using XTMon.Models;

namespace XTMon.Services;

public interface IDeploymentCheckService
{
    Task<DiagnosticsReport> RunCheckAsync(CancellationToken cancellationToken);
}
