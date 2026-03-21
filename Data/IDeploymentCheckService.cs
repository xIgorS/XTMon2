using XTMon.Models;

namespace XTMon.Data;

public interface IDeploymentCheckService
{
    Task<DiagnosticsReport> RunCheckAsync(CancellationToken cancellationToken);
}
