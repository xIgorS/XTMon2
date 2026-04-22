using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XTMon.Models;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class StartupDiagnosticsStateTests
{
    [Fact]
    public async Task RunAsync_WhenDiagnosticsPass_StoresHealthyReport()
    {
        var service = new Mock<IDeploymentCheckService>();
        service
            .Setup(checkService => checkService.RunCheckAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateReport(allPassed: true));

        var state = new StartupDiagnosticsState(service.Object, NullLogger<StartupDiagnosticsState>.Instance);
        var notifications = 0;
        state.StatusChanged += () => notifications++;

        await state.RunAsync(CancellationToken.None);

        Assert.True(state.HasCompleted);
        Assert.False(state.IsRunning);
        Assert.True(state.IsHealthy);
        Assert.NotNull(state.Report);
        Assert.Null(state.Error);
        Assert.True(notifications >= 2);
    }

    [Fact]
    public async Task RunAsync_WhenDiagnosticsThrow_SetsErrorState()
    {
        var service = new Mock<IDeploymentCheckService>();
        service
            .Setup(checkService => checkService.RunCheckAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));

        var state = new StartupDiagnosticsState(service.Object, NullLogger<StartupDiagnosticsState>.Instance);

        await state.RunAsync(CancellationToken.None);

        Assert.True(state.HasCompleted);
        Assert.False(state.IsRunning);
        Assert.False(state.IsHealthy);
        Assert.Null(state.Report);
        Assert.False(string.IsNullOrWhiteSpace(state.Error));
    }

    [Fact]
    public async Task RunAsync_WhenDiagnosticsRunSlow_RaisesSlowStateBeforeCompletion()
    {
        var completionSource = new TaskCompletionSource<DiagnosticsReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var slowStateSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var service = new Mock<IDeploymentCheckService>();
        service
            .Setup(checkService => checkService.RunCheckAsync(It.IsAny<CancellationToken>()))
            .Returns(() => completionSource.Task);

        var state = new StartupDiagnosticsState(
            service.Object,
            NullLogger<StartupDiagnosticsState>.Instance,
            TimeSpan.FromMilliseconds(25));

        state.StatusChanged += () =>
        {
            if (state.IsSlow)
            {
                slowStateSource.TrySetResult(true);
            }
        };

        var runTask = state.RunAsync(CancellationToken.None);

        await slowStateSource.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(state.IsRunning);
        Assert.True(state.IsSlow);

        completionSource.TrySetResult(CreateReport(allPassed: true));
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.False(state.IsRunning);
        Assert.False(state.IsSlow);
        Assert.True(state.IsHealthy);
    }

    private static DiagnosticsReport CreateReport(bool allPassed)
    {
        var database = new DatabaseCheckResult(
            ConnectionStringName: "LogFiAlmt",
            ServerName: "server",
            DatabaseName: "db",
            Connected: true,
            Duration: TimeSpan.FromSeconds(1),
            ConnectionError: null,
            StoredProcedures: [
                new StoredProcedureCheckResult(
                    FullName: "monitoring.UspMonitoringJobTakeNext",
                    Exists: allPassed,
                    Parameters: [],
                    Error: allPassed ? null : "Missing")
            ]);

        return new DiagnosticsReport(DateTimeOffset.UtcNow, [database]);
    }
}