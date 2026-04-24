using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Services;

public sealed class ReplayFlowsNavAlertState
{
    private readonly PnlDateState _pnlDateState;
    private readonly IReplayFlowRepository _replayFlowRepository;
    private readonly ILogger<ReplayFlowsNavAlertState>? _logger;
    private DateOnly? _trackedPnlDate;
    private DataValidationNavRunState _status;

    public event Action? StatusChanged;

    public ReplayFlowsNavAlertState(
        PnlDateState pnlDateState,
        IReplayFlowRepository replayFlowRepository,
        ILogger<ReplayFlowsNavAlertState>? logger = null)
    {
        _pnlDateState = pnlDateState;
        _replayFlowRepository = replayFlowRepository;
        _logger = logger;
        _status = DataValidationNavRunState.NotRun;
    }

    public DataValidationNavRunState GetStatus()
    {
        if (!_pnlDateState.SelectedDate.HasValue || _trackedPnlDate != _pnlDateState.SelectedDate.Value)
        {
            return DataValidationNavRunState.NotRun;
        }

        return _status;
    }

    public void ApplyStatus(
        DateOnly? pnlDate,
        IReadOnlyCollection<FailedFlowRow> failedRows,
        IReadOnlyCollection<ReplayFlowStatusRow> statusRows)
    {
        _trackedPnlDate = pnlDate;
        _status = BuildStatus(pnlDate, failedRows, statusRows);
        StatusChanged?.Invoke();
    }

    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (!_pnlDateState.SelectedDate.HasValue)
            {
                ApplyStatus(null, Array.Empty<FailedFlowRow>(), Array.Empty<ReplayFlowStatusRow>());
                return;
            }

            var pnlDate = _pnlDateState.SelectedDate.Value;
            var statusRows = await _replayFlowRepository.GetReplayFlowStatusAsync(pnlDate, cancellationToken);
            // Nav alerts don't need the failed-flow grid; only Alert-state detection cares about it.
            // When statusRows contain InProgress → Running; any completed non-success row → Alert via empty failedRows.
            ApplyStatus(pnlDate, Array.Empty<FailedFlowRow>(), statusRows);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Unable to refresh replay flows nav status.");
        }
    }

    private static DataValidationNavRunState BuildStatus(
        DateOnly? pnlDate,
        IReadOnlyCollection<FailedFlowRow> failedRows,
        IReadOnlyCollection<ReplayFlowStatusRow> statusRows)
    {
        if (!pnlDate.HasValue)
        {
            return DataValidationNavRunState.NotRun;
        }

        if (statusRows.Any(row => ReplayFlowsHelper.GetStatusKind(row) is ReplayStatusKind.Pending or ReplayStatusKind.InProgress))
        {
            return DataValidationNavRunState.Running;
        }

        if (failedRows.Count > 0)
        {
            return DataValidationNavRunState.Alert;
        }

        return DataValidationNavRunState.Succeeded;
    }
}
