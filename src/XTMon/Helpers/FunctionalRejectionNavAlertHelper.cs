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

        if (string.Equals(job.Status, "Failed", StringComparison.OrdinalIgnoreCase) || job.FailedAt is not null)
        {
            return DataValidationNavRunState.Failed;
        }

        if (string.Equals(job.Status, "Completed", StringComparison.OrdinalIgnoreCase) || job.CompletedAt is not null)
        {
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
