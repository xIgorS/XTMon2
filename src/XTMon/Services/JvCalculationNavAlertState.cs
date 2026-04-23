using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Services;

public sealed class JvCalculationNavAlertState
{
    private readonly PnlDateState _pnlDateState;
    private DateOnly? _trackedPnlDate;
    private DataValidationNavRunState _status;

    public event Action? StatusChanged;

    public JvCalculationNavAlertState(PnlDateState pnlDateState)
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

    public void ApplyStatus(DateOnly? pnlDate, JvJobRecord? job)
    {
        _trackedPnlDate = pnlDate;
        _status = BuildStatus(pnlDate, job);
        StatusChanged?.Invoke();
    }

    private static DataValidationNavRunState BuildStatus(DateOnly? pnlDate, JvJobRecord? job)
    {
        if (!pnlDate.HasValue || job is null || job.PnlDate != pnlDate.Value)
        {
            return DataValidationNavRunState.NotRun;
        }

        if (MonitoringJobHelper.ShouldTreatAsNotRun(job.Status, job.StartedAt))
        {
            return DataValidationNavRunState.NotRun;
        }

        if (MonitoringJobHelper.IsFailedStatus(job.Status)
            || (job.FailedAt.HasValue && !MonitoringJobHelper.IsCancelledStatus(job.Status)))
        {
            return DataValidationNavRunState.Failed;
        }

        if (MonitoringJobHelper.IsCancelledStatus(job.Status))
        {
            return DataValidationNavRunState.Cancelled;
        }

        if (MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return DataValidationNavRunState.Running;
        }

        if (MonitoringJobHelper.IsCompletedStatus(job.Status) || job.CompletedAt is not null)
        {
            var table = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
            return table is { Rows.Count: > 0 }
                ? DataValidationNavRunState.Alert
                : DataValidationNavRunState.Succeeded;
        }

        return DataValidationNavRunState.NotRun;
    }
}