using XTMon.Helpers;
using XTMon.Services;

namespace XTMon.Tests.Helpers;

public class SystemDiagnosticsBulkCancellationHelperTests
{
    [Fact]
    public void BuildMessage_WhenNothingCancelledAndNothingActive_ReturnsNoActiveMessage()
    {
        var message = SystemDiagnosticsBulkCancellationHelper.BuildMessage(new BackgroundJobBulkCancellationResult(0, 0, 0, 0, 0, 0));

        Assert.Equal("No active monitoring or JV background jobs were found.", message);
    }

    [Fact]
    public void BuildMessage_WhenActiveJobsRemain_DoesNotReportNoActiveJobs()
    {
        var message = SystemDiagnosticsBulkCancellationHelper.BuildMessage(new BackgroundJobBulkCancellationResult(0, 0, 1, 0, 1, 0));

        Assert.Contains("Status verification still reports 1 active job(s)", message, StringComparison.Ordinal);
        Assert.DoesNotContain("No active monitoring or JV background jobs were found.", message, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildMessage_WhenCancellationConfirmed_ReturnsSuccessSummary()
    {
        var message = SystemDiagnosticsBulkCancellationHelper.BuildMessage(new BackgroundJobBulkCancellationResult(2, 3, 1, 2, 0, 0));

        Assert.Equal("Marked 2 monitoring job(s) and 3 JV job(s) as cancelled. Cancellation was requested for 3 active worker(s). Status verification shows no active background jobs remain.", message);
    }
}