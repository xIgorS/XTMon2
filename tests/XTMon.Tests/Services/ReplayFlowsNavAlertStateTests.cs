using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class ReplayFlowsNavAlertStateTests
{
    private static readonly DateOnly TestDate = new(2026, 4, 23);
    private static readonly DateOnly OtherDate = new(2026, 4, 22);

    [Fact]
    public void ApplyStatus_UsesBlueRedGreenOrangeRules()
    {
        var pnlDateState = new PnlDateState();
        pnlDateState.SetDate(TestDate);
        var state = new ReplayFlowsNavAlertState(pnlDateState, Mock.Of<IReplayFlowRepository>());

        state.ApplyStatus(null, Array.Empty<FailedFlowRow>(), Array.Empty<ReplayFlowStatusRow>());
        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus());

        state.ApplyStatus(TestDate, [CreateFailedFlowRow()], Array.Empty<ReplayFlowStatusRow>());
        Assert.Equal(DataValidationNavRunState.Alert, state.GetStatus());

        state.ApplyStatus(TestDate, Array.Empty<FailedFlowRow>(), Array.Empty<ReplayFlowStatusRow>());
        Assert.Equal(DataValidationNavRunState.Succeeded, state.GetStatus());

        state.ApplyStatus(TestDate, [CreateFailedFlowRow()], [CreateStatusRow("Processing")]);
        Assert.Equal(DataValidationNavRunState.Running, state.GetStatus());
    }

    [Fact]
    public void GetStatus_ReturnsNotRun_WhenTrackedDateDoesNotMatchSelectedDate()
    {
        var pnlDateState = new PnlDateState();
        pnlDateState.SetDate(TestDate);
        var state = new ReplayFlowsNavAlertState(pnlDateState, Mock.Of<IReplayFlowRepository>());

        state.ApplyStatus(OtherDate, [CreateFailedFlowRow(OtherDate)], Array.Empty<ReplayFlowStatusRow>());

        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus());
    }

    private static FailedFlowRow CreateFailedFlowRow(DateOnly? pnlDate = null)
    {
        return new FailedFlowRow(
            FlowId: 1,
            FlowIdDerivedFrom: 2,
            PnlDate: pnlDate ?? TestDate,
            ArrivalDate: DateTime.UtcNow,
            BusinessDataType: "Pricing",
            FeedSource: "FOCUS",
            CurrentStep: "FAILED",
            IsFailed: true,
            TypeOfCalculation: "EOD",
            IsAdjustment: false,
            IsReplay: false,
            WithBackdated: false,
            SkipCoreProcess: false,
            DropTableTmp: false,
            PackageGuid: Guid.NewGuid(),
            FileName: "file.csv");
    }

    private static ReplayFlowStatusRow CreateStatusRow(string status)
    {
        return new ReplayFlowStatusRow(
            FlowId: 1,
            FlowIdDerivedFrom: 2,
            PnlDate: TestDate,
            PackageGuid: Guid.NewGuid(),
            WithBackdated: false,
            SkipCoreProcess: false,
            DropTableTmp: false,
            DateCreated: DateTime.UtcNow,
            CreatedBy: "user",
            DateSubmitted: DateTime.UtcNow,
            DateStarted: DateTime.UtcNow,
            DateCompleted: null,
            Status: status,
            ProcessStatus: null,
            Duration: null);
    }
}