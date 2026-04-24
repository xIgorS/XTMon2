using XTMon.Models;

namespace XTMon.Helpers;

internal static class SystemDiagnosticsRecoveryHelper
{
    public static string BuildRecoveryMessage(StartupJobRecoveryResult result)
    {
        if (result.TotalRecoveredJobs == 0)
        {
            return "No running monitoring jobs, JV jobs, or in-progress replay batches were found to reset.";
        }

        return $"Reset {result.RecoveredMonitoringJobs} monitoring job(s), {result.RecoveredJvJobs} JV job(s), and {result.RecoveredReplayBatches} replay batch row(s) that were still marked active by an earlier application instance.";
    }
}