using XTMon.Models;

namespace XTMon.Data;

public interface IReplayFlowRepository
{
    Task<IReadOnlyList<FailedFlowRow>> GetFailedFlowsAsync(DateOnly? pnlDate, string? replayFlowSet, CancellationToken cancellationToken);
    Task<IReadOnlyList<ReplayFlowResultRow>> ReplayFlowsAsync(IReadOnlyCollection<ReplayFlowSubmissionRow> rows, string userId, CancellationToken cancellationToken);
    Task ProcessReplayFlowsAsync(CancellationToken cancellationToken);
    Task<IReadOnlyList<ReplayFlowStatusRow>> GetReplayFlowStatusAsync(DateOnly? pnlDate, CancellationToken cancellationToken);
    Task RefreshReplayFlowProcessStatusAsync(CancellationToken cancellationToken);
}
