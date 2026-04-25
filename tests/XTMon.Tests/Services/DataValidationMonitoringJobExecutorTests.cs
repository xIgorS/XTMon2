using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class DataValidationMonitoringJobExecutorTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    [Fact]
    public void CanExecute_ReturnsFalse_ForBatchStatus()
    {
        var executor = new DataValidationMonitoringJobExecutor(Mock.Of<IServiceProvider>());
        var job = MakeJob(MonitoringJobHelper.BatchStatusSubmenuKey);

        var canExecute = executor.CanExecute(job);

        Assert.False(canExecute);
    }

    [Fact]
    public void CanExecute_ReturnsTrue_ForOwnedDataValidationSubmenu()
    {
        var executor = new DataValidationMonitoringJobExecutor(Mock.Of<IServiceProvider>());
        var job = MakeJob("daily-balance");

        var canExecute = executor.CanExecute(job);

        Assert.True(canExecute);
    }

    private static MonitoringJobRecord MakeJob(string submenuKey)
    {
        return new MonitoringJobRecord(
            JobId: 1L,
            Category: MonitoringJobHelper.DataValidationCategory,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: TestDate,
            Status: MonitoringJobHelper.QueuedStatus,
            WorkerId: null,
            ParametersJson: null,
            ParameterSummary: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: null,
            LastHeartbeatAt: null,
            CompletedAt: null,
            FailedAt: null,
            ErrorMessage: null,
            ParsedQuery: null,
            GridColumnsJson: null,
            GridRowsJson: null,
            MetadataJson: null,
            SavedAt: null);
    }
}