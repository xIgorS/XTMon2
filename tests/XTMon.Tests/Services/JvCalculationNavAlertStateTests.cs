using XTMon.Helpers;
using XTMon.Models;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class JvCalculationNavAlertStateTests
{
    private static readonly DateOnly TestDate = new(2026, 4, 23);
    private static readonly DateOnly OtherDate = new(2026, 4, 22);

    [Fact]
    public void ApplyStatus_UsesBlueRedGreenOrangeRules()
    {
        var pnlDateState = new PnlDateState();
        pnlDateState.SetDate(TestDate);
        var state = new JvCalculationNavAlertState(pnlDateState);

        state.ApplyStatus(TestDate, job: null);
        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus());

        state.ApplyStatus(TestDate, CreateJob("Running", rowsJson: null));
        Assert.Equal(DataValidationNavRunState.Running, state.GetStatus());

        state.ApplyStatus(TestDate, CreateJob("Completed", rowsJson: "[[\"issue\"]]"));
        Assert.Equal(DataValidationNavRunState.Alert, state.GetStatus());

        state.ApplyStatus(TestDate, CreateJob("Completed", rowsJson: "[]"));
        Assert.Equal(DataValidationNavRunState.Succeeded, state.GetStatus());
    }

    [Fact]
    public void GetStatus_ReturnsNotRun_WhenTrackedDateDoesNotMatchSelectedDate()
    {
        var pnlDateState = new PnlDateState();
        pnlDateState.SetDate(TestDate);
        var state = new JvCalculationNavAlertState(pnlDateState);

        state.ApplyStatus(OtherDate, CreateJob("Completed", rowsJson: "[]", pnlDate: OtherDate));

        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus());
    }

    [Fact]
    public void ApplyStatus_TreatsCancelledAsBlueNotRunState_WhenJobNeverStarted()
    {
        var pnlDateState = new PnlDateState();
        pnlDateState.SetDate(TestDate);
        var state = new JvCalculationNavAlertState(pnlDateState);

        state.ApplyStatus(TestDate, CreateJob("Cancelled", rowsJson: null));

        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus());
    }

    private static JvJobRecord CreateJob(string status, string? rowsJson, DateOnly? pnlDate = null)
    {
        var now = DateTime.UtcNow;
        return new JvJobRecord(
            JobId: 1,
            UserId: "user",
            PnlDate: pnlDate ?? TestDate,
            RequestType: "CheckOnly",
            Status: status,
            WorkerId: null,
            EnqueuedAt: now,
            StartedAt: status is "Queued" or "Cancelled" ? null : now,
            LastHeartbeatAt: now,
            CompletedAt: string.Equals(status, "Completed", StringComparison.OrdinalIgnoreCase) ? now : null,
            FailedAt: string.Equals(status, "Failed", StringComparison.OrdinalIgnoreCase) ? now : null,
            ErrorMessage: null,
            QueryCheck: null,
            QueryFix: null,
            GridColumnsJson: rowsJson is null ? null : "[\"Result\"]",
            GridRowsJson: rowsJson,
            SavedAt: now);
    }
}