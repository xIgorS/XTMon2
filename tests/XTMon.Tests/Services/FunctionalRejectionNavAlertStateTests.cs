using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class FunctionalRejectionNavAlertStateTests
{
    private static readonly DateOnly TestDate = new(2026, 4, 21);
    private static readonly DateOnly OtherDate = new(2026, 4, 20);

    [Fact]
    public async Task RefreshAsync_LoadsSelectedDateAlerts()
    {
        var pnlDateState = new PnlDateState();

        var pnlDateRepository = new Mock<IJvCalculationRepository>();
        pnlDateRepository
            .Setup(repository => repository.GetJvPnlDatesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new JvPnlDatesResult(TestDate, [TestDate, OtherDate]));

        var monitoringJobRepository = new Mock<IMonitoringJobRepository>();
        monitoringJobRepository
            .Setup(repository => repository.GetLatestMonitoringJobsByCategoryAsync(MonitoringJobHelper.FunctionalRejectionCategory, TestDate, It.IsAny<CancellationToken>()))
            .ReturnsAsync([
                CreateJob(BuildKey(1, "FOCUS", "STAGING", "FOCUS"), "Failed", TestDate),
                CreateJob(BuildKey(2, "ALG", "DTM", "ALG"), "Completed", TestDate)
            ]);

        var state = new FunctionalRejectionNavAlertState(
            pnlDateState,
            pnlDateRepository.Object,
            monitoringJobRepository.Object,
            NullLogger<FunctionalRejectionNavAlertState>.Instance);

        await state.RefreshAsync(CancellationToken.None);

        Assert.Equal(DataValidationNavRunState.Failed, state.GetStatus(BuildKey(1, "FOCUS", "STAGING", "FOCUS")));
        Assert.Equal(DataValidationNavRunState.Succeeded, state.GetStatus(BuildKey(2, "ALG", "DTM", "ALG")));
        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus(BuildKey(3, "CAL2", "STAGING", "CAL2")));
        monitoringJobRepository.Verify(
            repository => repository.GetLatestMonitoringJobsByCategoryAsync(MonitoringJobHelper.FunctionalRejectionCategory, TestDate, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public void ApplyStatuses_IgnoresJobsFromOtherPnlDates()
    {
        var state = new FunctionalRejectionNavAlertState(
            new PnlDateState(),
            Mock.Of<IJvCalculationRepository>(),
            Mock.Of<IMonitoringJobRepository>(),
            NullLogger<FunctionalRejectionNavAlertState>.Instance);

        var currentKey = BuildKey(1, "FOCUS", "STAGING", "FOCUS");
        var otherKey = BuildKey(2, "ALG", "DTM", "ALG");

        state.ApplyStatuses(TestDate, [
            CreateJob(currentKey, "Running", TestDate),
            CreateJob(otherKey, "Failed", OtherDate)
        ]);

        Assert.Equal(DataValidationNavRunState.Running, state.GetStatus(currentKey));
        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus(otherKey));
    }

    [Fact]
    public async Task RefreshAsync_WhenRepositoryThrows_PreservesExistingStatuses()
    {
        var pnlDateState = new PnlDateState();

        var pnlDateRepository = new Mock<IJvCalculationRepository>();
        pnlDateRepository
            .Setup(repository => repository.GetJvPnlDatesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new JvPnlDatesResult(TestDate, [TestDate, OtherDate]));

        var monitoringJobRepository = new Mock<IMonitoringJobRepository>();
        monitoringJobRepository
            .Setup(repository => repository.GetLatestMonitoringJobsByCategoryAsync(MonitoringJobHelper.FunctionalRejectionCategory, TestDate, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Transient SQL failure"));

        var state = new FunctionalRejectionNavAlertState(
            pnlDateState,
            pnlDateRepository.Object,
            monitoringJobRepository.Object,
            NullLogger<FunctionalRejectionNavAlertState>.Instance);

        var currentKey = BuildKey(1, "FOCUS", "STAGING", "FOCUS");
        state.ApplyStatuses(TestDate, [
            CreateJob(currentKey, "Completed", TestDate)
        ]);

        await state.RefreshAsync(CancellationToken.None);

        Assert.Equal(DataValidationNavRunState.Succeeded, state.GetStatus(currentKey));
        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus(BuildKey(3, "CAL2", "STAGING", "CAL2")));
    }

    private static MonitoringJobRecord CreateJob(string submenuKey, string status, DateOnly pnlDate)
    {
        return new MonitoringJobRecord(
            JobId: 1,
            Category: MonitoringJobHelper.FunctionalRejectionCategory,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: pnlDate,
            Status: status,
            WorkerId: null,
            ParametersJson: null,
            ParameterSummary: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: DateTime.UtcNow,
            LastHeartbeatAt: DateTime.UtcNow,
            CompletedAt: string.Equals(status, "Completed", StringComparison.OrdinalIgnoreCase) ? DateTime.UtcNow : null,
            FailedAt: string.Equals(status, "Failed", StringComparison.OrdinalIgnoreCase) ? DateTime.UtcNow : null,
            ErrorMessage: null,
            ParsedQuery: "SELECT 1",
            GridColumnsJson: null,
            GridRowsJson: null,
            MetadataJson: null,
            SavedAt: DateTime.UtcNow);
    }

    private static string BuildKey(int businessDataTypeId, string sourceSystemName, string dbConnection, string sourceSystemBusinessDataTypeCode)
    {
        return MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(
            businessDataTypeId,
            sourceSystemName,
            dbConnection,
            sourceSystemBusinessDataTypeCode);
    }
}