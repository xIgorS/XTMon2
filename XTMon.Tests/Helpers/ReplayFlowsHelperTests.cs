using XTMon.Data;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class ReplayFlowsHelperTests
{
    // ─── TryNormalizeReplayFlowSet ───────────────────────────────────────────────

    [Fact]
    public void TryNormalizeReplayFlowSet_WhenNull_ReturnsTrueAndNullNormalized()
    {
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet(null, out var normalized);
        Assert.True(result);
        Assert.Null(normalized);
    }

    [Fact]
    public void TryNormalizeReplayFlowSet_WhenEmpty_ReturnsTrueAndNullNormalized()
    {
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet("   ", out var normalized);
        Assert.True(result);
        Assert.Null(normalized);
    }

    [Fact]
    public void TryNormalizeReplayFlowSet_SingleInteger_ReturnsNormalized()
    {
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet("42", out var normalized);
        Assert.True(result);
        Assert.Equal("42", normalized);
    }

    [Fact]
    public void TryNormalizeReplayFlowSet_CommaSeparatedIntegers_TrimsAndJoins()
    {
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet(" 1 , 2 , 3 ", out var normalized);
        Assert.True(result);
        Assert.Equal("1,2,3", normalized);
    }

    [Fact]
    public void TryNormalizeReplayFlowSet_WithNonInteger_ReturnsFalse()
    {
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet("1,abc,3", out _);
        Assert.False(result);
    }

    [Fact]
    public void TryNormalizeReplayFlowSet_WithNegativeInteger_ReturnsFalse()
    {
        // NumberStyles.None rejects negative signs
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet("-5", out _);
        Assert.False(result);
    }

    [Fact]
    public void TryNormalizeReplayFlowSet_TrailingComma_ReturnsFalse()
    {
        var result = ReplayFlowsHelper.TryNormalizeReplayFlowSet("1,2,", out _);
        Assert.False(result);
    }

    // ─── GetStatusKind — completed statuses ──────────────────────────────────────

    [Theory]
    [InlineData("completed")]
    [InlineData("Completed")]
    [InlineData("COMPLETED")]
    [InlineData("submissioncompleted")]
    [InlineData("SubmissionCompleted")]
    [InlineData("submission_completed")]   // separators stripped
    [InlineData("submission-completed")]
    [InlineData("done")]
    [InlineData("Done")]
    [InlineData("success")]
    [InlineData("SUCCESS")]
    public void GetStatusKind_CompletedStatuses_ReturnsCompleted(string status)
    {
        var row = MakeStatusRow(status);
        Assert.Equal(ReplayStatusKind.Completed, ReplayFlowsHelper.GetStatusKind(row));
    }

    // ─── GetStatusKind — in-progress statuses ────────────────────────────────────

    [Theory]
    [InlineData("inprogress")]
    [InlineData("InProgress")]
    [InlineData("in_progress")]
    [InlineData("in-progress")]
    [InlineData("submissionstarted")]
    [InlineData("SubmissionStarted")]
    [InlineData("submissonstarted")]       // intentional DB typo
    [InlineData("processing")]
    [InlineData("Processing")]
    [InlineData("running")]
    [InlineData("RUNNING")]
    public void GetStatusKind_InProgressStatuses_ReturnsInProgress(string status)
    {
        var row = MakeStatusRow(status);
        Assert.Equal(ReplayStatusKind.InProgress, ReplayFlowsHelper.GetStatusKind(row));
    }

    // ─── GetStatusKind — pending statuses ────────────────────────────────────────

    [Theory]
    [InlineData("pending")]
    [InlineData("Pending")]
    [InlineData("inserted")]
    [InlineData("Inserted")]
    [InlineData("queued")]
    [InlineData("Queued")]
    [InlineData("new")]
    [InlineData("NEW")]
    public void GetStatusKind_PendingStatuses_ReturnsPending(string status)
    {
        var row = MakeStatusRow(status);
        Assert.Equal(ReplayStatusKind.Pending, ReplayFlowsHelper.GetStatusKind(row));
    }

    // ─── GetStatusKind — fallback to dates ───────────────────────────────────────

    [Fact]
    public void GetStatusKind_NoStatusButDateCompleted_ReturnsCompleted()
    {
        var row = MakeStatusRow(null, dateStarted: DateTime.UtcNow.AddHours(-1), dateCompleted: DateTime.UtcNow);
        Assert.Equal(ReplayStatusKind.Completed, ReplayFlowsHelper.GetStatusKind(row));
    }

    [Fact]
    public void GetStatusKind_NoStatusButDateStarted_ReturnsInProgress()
    {
        var row = MakeStatusRow(null, dateStarted: DateTime.UtcNow.AddMinutes(-5));
        Assert.Equal(ReplayStatusKind.InProgress, ReplayFlowsHelper.GetStatusKind(row));
    }

    [Fact]
    public void GetStatusKind_NoDates_ReturnsPending()
    {
        var row = MakeStatusRow(null);
        Assert.Equal(ReplayStatusKind.Pending, ReplayFlowsHelper.GetStatusKind(row));
    }

    [Fact]
    public void GetStatusKind_UnrecognisedStatusNoDates_ReturnsPending()
    {
        var row = MakeStatusRow("Unknown");
        Assert.Equal(ReplayStatusKind.Pending, ReplayFlowsHelper.GetStatusKind(row));
    }

    // ─── FormatDate ──────────────────────────────────────────────────────────────

    [Fact]
    public void FormatDate_WhenNull_ReturnsDash()
    {
        Assert.Equal("-", ReplayFlowsHelper.FormatDate(null));
    }

    [Fact]
    public void FormatDate_WhenValue_ReturnsFormattedDate()
    {
        Assert.Equal("15-03-2025", ReplayFlowsHelper.FormatDate(new DateOnly(2025, 3, 15)));
    }

    // ─── FormatNumber ────────────────────────────────────────────────────────────

    [Fact]
    public void FormatNumber_WhenNull_ReturnsDash()
    {
        Assert.Equal("-", ReplayFlowsHelper.FormatNumber(null));
    }

    [Fact]
    public void FormatNumber_WhenValue_ReturnsSpaceSeparated()
    {
        Assert.Equal("1 000 000", ReplayFlowsHelper.FormatNumber(1_000_000));
    }

    [Fact]
    public void FormatNumber_WhenSmallValue_ReturnsNoSeparator()
    {
        Assert.Equal("42", ReplayFlowsHelper.FormatNumber(42L));
    }

    // ─── FormatDuration ──────────────────────────────────────────────────────────

    [Fact]
    public void FormatDuration_WhenRawDurationPresent_ReturnsItAsIs()
    {
        Assert.Equal("5m 30s", ReplayFlowsHelper.FormatDuration("5m 30s", null, null));
    }

    [Fact]
    public void FormatDuration_WhenNoDateStarted_ReturnsDash()
    {
        Assert.Equal("-", ReplayFlowsHelper.FormatDuration(null, null, null));
    }

    [Fact]
    public void FormatDuration_WhenDurationUnderOneMinute_ReturnsSeconds()
    {
        var start = new DateTime(2025, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2025, 1, 1, 10, 0, 45, DateTimeKind.Utc);
        Assert.Equal("45s", ReplayFlowsHelper.FormatDuration(null, start, end));
    }

    [Fact]
    public void FormatDuration_WhenDurationOverOneMinute_ReturnsMinutesSeconds()
    {
        var start = new DateTime(2025, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2025, 1, 1, 10, 3, 15, DateTimeKind.Utc);
        Assert.Equal("3m 15s", ReplayFlowsHelper.FormatDuration(null, start, end));
    }

    [Fact]
    public void FormatDuration_WhenDurationOverOneHour_ReturnsHoursMinutesSeconds()
    {
        var start = new DateTime(2025, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2025, 1, 1, 12, 5, 30, DateTimeKind.Utc);
        Assert.Equal("2h 05m 30s", ReplayFlowsHelper.FormatDuration(null, start, end));
    }

    [Fact]
    public void FormatDuration_WhenNegativeDuration_ReturnsZeroSeconds()
    {
        // Clock skew guard: completed before started
        var start = new DateTime(2025, 1, 1, 10, 0, 5, DateTimeKind.Utc);
        var end = new DateTime(2025, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        Assert.Equal("0s", ReplayFlowsHelper.FormatDuration(null, start, end));
    }

    // ─── Helpers ────────────────────────────────────────────────────────────────

    private static ReplayFlowStatusRow MakeStatusRow(
        string? status,
        DateTime? dateStarted = null,
        DateTime? dateCompleted = null) =>
        new ReplayFlowStatusRow(
            FlowId: 1L,
            FlowIdDerivedFrom: 2L,
            PnlDate: new DateOnly(2025, 1, 1),
            PackageGuid: Guid.Empty,
            WithBackdated: false,
            SkipCoreProcess: false,
            DropTableTmp: false,
            DateCreated: DateTime.UtcNow,
            CreatedBy: "user1",
            DateSubmitted: null,
            DateStarted: dateStarted,
            DateCompleted: dateCompleted,
            Status: status,
            ProcessStatus: null,
            Duration: null);
}
