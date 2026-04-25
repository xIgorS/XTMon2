using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using XTMon.Components.Pages;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Components;

public class MonitoringJobPageBaseTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    [Fact]
    public void LoadError_And_RunError_AreIndependent_WhenSetDirectly()
    {
        var page = new TestMonitoringJobPage();

        page.SetLoadError("load");
        page.SetRunError("run");

        Assert.Equal("load", page.GetLoadError());
        Assert.Equal("run", page.GetRunError());
    }

    [Fact]
    public void ClearLoadedState_ClearsBothBackgroundErrors()
    {
        var page = new TestMonitoringJobPage();

        page.SetLoadError("load");
        page.SetRunError("run");
        page.ClearLoadedStateForTest();

        Assert.Null(page.GetLoadError());
        Assert.Null(page.GetRunError());
    }

    [Fact]
    public async Task StopPollingAsync_WaitsForInFlightRefreshToComplete()
    {
        var refreshStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRefreshCompletion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var repository = new Mock<IMonitoringJobRepository>();

        repository.Setup(repo => repo.GetMonitoringJobByIdAsync(42L, It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                refreshStarted.TrySetResult(true);
                await allowRefreshCompletion.Task;
                return CreateRunningJob(42L);
            });

        var page = CreatePage(repository.Object);
        page.SetActiveJobForTest(42L, "Running");

        await page.StartPollingForTestAsync();
        await refreshStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));

        var stopTask = page.StopPollingForTestAsync();

        Assert.False(stopTask.IsCompleted);

        allowRefreshCompletion.SetResult(true);

        await stopTask;
    }

    [Fact]
    public async Task DisposeAsync_WaitsForInFlightRefreshToComplete()
    {
        var refreshStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRefreshCompletion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var repository = new Mock<IMonitoringJobRepository>();

        repository.Setup(repo => repo.GetMonitoringJobByIdAsync(43L, It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                refreshStarted.TrySetResult(true);
                await allowRefreshCompletion.Task;
                return CreateRunningJob(43L);
            });

        var page = CreatePage(repository.Object);
        page.SetActiveJobForTest(43L, "Running");

        await page.StartPollingForTestAsync();
        await refreshStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));

        var disposeTask = page.DisposeAsync().AsTask();

        Assert.False(disposeTask.IsCompleted);

        allowRefreshCompletion.SetResult(true);

        await disposeTask;
    }

    private static TestMonitoringJobPage CreatePage(IMonitoringJobRepository repository)
    {
        var page = new TestMonitoringJobPage();
        page.InitializeForTest(
            repository,
            Mock.Of<IJvCalculationRepository>(),
            new PnlDateState(),
            Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions
            {
                JobPollIntervalSeconds = 1
            }),
            NullLogger<TestMonitoringJobPage>.Instance);

        return page;
    }

    private static MonitoringJobRecord CreateRunningJob(long jobId) => new(
        JobId: jobId,
        Category: MonitoringJobHelper.DataValidationCategory,
        SubmenuKey: "test-monitoring-job",
        DisplayName: "Test Monitoring Job",
        PnlDate: TestDate,
        Status: "Running",
        WorkerId: null,
        ParametersJson: null,
        ParameterSummary: null,
        EnqueuedAt: DateTime.UtcNow,
        StartedAt: DateTime.UtcNow,
        LastHeartbeatAt: null,
        CompletedAt: null,
        FailedAt: null,
        ErrorMessage: null,
        ParsedQuery: null,
        GridColumnsJson: null,
        GridRowsJson: null,
        MetadataJson: null,
        SavedAt: null);

    private sealed class TestMonitoringJobPage : MonitoringJobPageBase<TestMonitoringJobPage>
    {
        protected override string MonitoringSubmenuKey => "test-monitoring-job";

        protected override string MonitoringJobName => "Test Monitoring Job";

        protected override string DefaultLoadErrorMessage => "Unable to load test monitoring job.";

        public void InitializeForTest(
            IMonitoringJobRepository monitoringJobRepository,
            IJvCalculationRepository pnlDateRepository,
            PnlDateState pnlDateState,
            IOptions<MonitoringJobsOptions> monitoringJobsOptions,
            ILogger<TestMonitoringJobPage> logger)
        {
            MonitoringJobRepository = monitoringJobRepository;
            PnlDateRepository = pnlDateRepository;
            PnlDateState = pnlDateState;
            MonitoringJobsOptions = monitoringJobsOptions;
            Logger = logger;
        }

        public string? GetLoadError() => loadError;

        public string? GetRunError() => runError;

        public void SetLoadError(string? value) => loadError = value;

        public void SetRunError(string? value) => runError = value;

        public void ClearLoadedStateForTest() => ClearLoadedState();

        public void SetActiveJobForTest(long jobId, string status)
        {
            activeJobId = jobId;
            activeJobStatus = status;
        }

        public Task StartPollingForTestAsync() => StartPollingIfNeededAsync();

        public Task StopPollingForTestAsync() => StopPollingAsync();

        protected override Task RequestStateHasChangedAsync() => Task.CompletedTask;

        protected override void ApplyJobCore(MonitoringJobRecord job)
        {
        }

        protected override void ClearLoadedStateCore()
        {
        }

        protected override bool HasLoadedResult() => false;
    }
}