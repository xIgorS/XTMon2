using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Services;

public sealed class ReplayFlowsNavAlertState
{
    private readonly PnlDateState _pnlDateState;
    private DateOnly? _trackedPnlDate;
    private DataValidationNavRunState _status;

    public event Action? StatusChanged;

    public ReplayFlowsNavAlertState(PnlDateState pnlDateState)
    {
        _pnlDateState = pnlDateState;
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