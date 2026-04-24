using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class SystemDiagnosticsRecoveryHelperTests
{
    [Fact]
    public void BuildRecoveryMessage_WhenNothingRecovered_ReturnsEmptySummary()
    {
        var message = SystemDiagnosticsRecoveryHelper.BuildRecoveryMessage(new StartupJobRecoveryResult(0, 0, 0));

        Assert.Equal("No running monitoring jobs, JV jobs, or in-progress replay batches were found to reset.", message);
    }

    [Fact]
    public void BuildRecoveryMessage_WhenJobsRecovered_IncludesReplayCount()
    {
        var message = SystemDiagnosticsRecoveryHelper.BuildRecoveryMessage(new StartupJobRecoveryResult(2, 1, 4));

        Assert.Equal("Reset 2 monitoring job(s), 1 JV job(s), and 4 replay batch row(s) that were still marked active by an earlier application instance.", message);
    }
}