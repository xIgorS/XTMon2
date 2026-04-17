using XTMon.Models;

namespace XTMon.Repositories;

public interface IMissingWorkflowCheckRepository
{
    Task<MissingWorkflowCheckResult> GetMissingWorkflowCheckAsync(DateOnly pnlDate, CancellationToken cancellationToken);
}