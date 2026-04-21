using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class DataValidationNavAlertStateTests
{
    private static readonly DateOnly TestDate = new(2026, 4, 21);

    [Fact]
    public async Task RefreshAsync_LoadsSelectedDateAlerts()
    {
        var pnlDateState = new PnlDateState();

        var pnlDateRepository = new Mock<IJvCalculationRepository>();
        pnlDateRepository
            .Setup(repository => repository.GetJvPnlDatesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new JvPnlDatesResult(TestDate, [TestDate]));

        var monitoringJobRepository = new Mock<IMonitoringJobRepository>();
        monitoringJobRepository
            .Setup(repository => repository.GetLatestMonitoringJobsByCategoryAsync(MonitoringJobHelper.DataValidationCategory, TestDate, It.IsAny<CancellationToken>()))
            .ReturnsAsync([
                CreateJob("batch-status", "Failed"),
                CreateJob("future-cash", "Completed")
            ]);

        var state = new DataValidationNavAlertState(
            pnlDateState,
            pnlDateRepository.Object,
            monitoringJobRepository.Object,
            NullLogger<DataValidationNavAlertState>.Instance);

        await state.RefreshAsync(CancellationToken.None);

        Assert.Equal(DataValidationNavRunState.Failed, state.GetStatus("batch-status"));
        Assert.Equal(DataValidationNavRunState.Succeeded, state.GetStatus("future-cash"));
        Assert.Equal(DataValidationNavRunState.NotRun, state.GetStatus("referential-data"));
        monitoringJobRepository.Verify(
            repository => repository.GetLatestMonitoringJobsByCategoryAsync(MonitoringJobHelper.DataValidationCategory, TestDate, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public void ApplyStatuses_UpdatesSupportedRoutes_AndRaisesChangeEvent()
    {
        var state = new DataValidationNavAlertState(
            new PnlDateState(),
            Mock.Of<IJvCalculationRepository>(),
            Mock.Of<IMonitoringJobRepository>(),
            NullLogger<DataValidationNavAlertState>.Instance);
        var changeNotifications = 0;

        state.StatusesChanged += () => changeNotifications++;

        state.ApplyStatuses(TestDate, [
            CreateJob("batch-status", "Running"),
            CreateJob("future-cash", "Completed")
        ]);

        Assert.Equal(DataValidationNavRunState.Running, state.GetStatus("batch-status"));
        Assert.Equal(DataValidationNavRunState.Succeeded, state.GetStatus("future-cash"));
        Assert.Equal(1, changeNotifications);
    }

    private static MonitoringJobRecord CreateJob(string submenuKey, string status)
    {
        return new MonitoringJobRecord(
            JobId: 1,
            Category: MonitoringJobHelper.DataValidationCategory,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: TestDate,
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
}