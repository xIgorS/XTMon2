using XTMon.Services;

namespace XTMon.Helpers;

internal static class SystemDiagnosticsBulkCancellationHelper
{
    public static string BuildMessage(BackgroundJobBulkCancellationResult result)
    {
        if (result.TotalJobsCancelled == 0 && result.TotalActiveJobsRemaining == 0)
        {
            return "No active monitoring or JV background jobs were found.";
        }

        var summary = $"Marked {result.MonitoringJobsCancelled} monitoring job(s) and {result.JvJobsCancelled} JV job(s) as cancelled.";
        var workers = $" Cancellation was requested for {result.TotalWorkersCancellationRequested} active worker(s).";

        if (result.CancellationConfirmed)
        {
            return summary + workers + " Status verification shows no active background jobs remain.";
        }

        return summary + workers + $" Status verification still reports {result.TotalActiveJobsRemaining} active job(s), so another refresh may be needed while long-running queries unwind.";
    }
}