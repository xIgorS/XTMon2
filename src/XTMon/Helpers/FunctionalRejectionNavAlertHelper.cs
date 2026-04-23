using XTMon.Models;

namespace XTMon.Helpers;

internal static class FunctionalRejectionNavAlertHelper
{
    public static DataValidationNavRunState GetRunState(MonitoringJobRecord? job)
    {
        if (job is null)
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

        if (MonitoringJobHelper.IsCompletedStatus(job.Status) || job.CompletedAt is not null)
        {
            if (MonitoringJobHelper.TryGetHasAlertsFromMetadata(job.MetadataJson, out var hasAlerts))
            {
                return hasAlerts ? DataValidationNavRunState.Alert : DataValidationNavRunState.Succeeded;
            }

            var table = JvCalculationHelper.DeserializeMonitoringTable(job.GridColumnsJson, job.GridRowsJson);
            return table is { Rows.Count: > 0 }
                ? DataValidationNavRunState.Alert
                : DataValidationNavRunState.Succeeded;
        }

        if (MonitoringJobHelper.IsActiveStatus(job.Status))
        {
            return DataValidationNavRunState.Running;
        }

        return DataValidationNavRunState.NotRun;
    }
}
