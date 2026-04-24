using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class JobDiagnosticsServiceTests
{
    [Fact]
    public async Task GetStuckJobsReportAsync_FiltersReplayRowsToConfiguredStaleThreshold()
    {
        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.GetStuckMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<MonitoringJobRecord>());

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.GetStuckJvJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<JvJobRecord>());

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.GetStuckReplayBatchesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[]
            {
                MakeReplayRow(ageSeconds: 899),
                MakeReplayRow(ageSeconds: 901)
            });

        var service = CreateService(
            monitoringRepository.Object,
            jvRepository.Object,
            replayRepository.Object,
            replayOptions: new ReplayFlowsOptions { RunningStaleTimeoutSeconds = 900 });

        var report = await service.GetStuckJobsReportAsync(CancellationToken.None);

        Assert.Single(report.StuckReplayBatches);
        Assert.Equal(901, report.StuckReplayBatches[0].AgeSeconds);
    }

    [Fact]
    public async Task ForceExpireAllStuckAsync_UsesSameThresholdsAsStuckPanel()
    {
        var monitoringThreshold = TimeSpan.Zero;
        var jvThreshold = TimeSpan.Zero;
        var replayThreshold = TimeSpan.Zero;

        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<TimeSpan, string, CancellationToken>((threshold, _, _) => monitoringThreshold = threshold)
            .ReturnsAsync(2);

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.ExpireStaleRunningJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<TimeSpan, string, CancellationToken>((threshold, _, _) => jvThreshold = threshold)
            .ReturnsAsync(3);

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.FailStaleReplayBatchesAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<TimeSpan, string, CancellationToken>((threshold, _, _) => replayThreshold = threshold)
            .ReturnsAsync(4);

        var service = CreateService(
            monitoringRepository.Object,
            jvRepository.Object,
            replayRepository.Object,
            monitoringOptions: new MonitoringJobsOptions { JobRunningStaleTimeoutSeconds = 900 },
            jvOptions: new JvCalculationOptions { JobRunningStaleTimeoutSeconds = 900 },
            replayOptions: new ReplayFlowsOptions { RunningStaleTimeoutSeconds = 900 });

        var result = await service.ForceExpireAllStuckAsync(CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(60), monitoringThreshold);
        Assert.Equal(TimeSpan.FromSeconds(60), jvThreshold);
        Assert.Equal(TimeSpan.FromSeconds(900), replayThreshold);
        Assert.Equal(new ForceExpireResult(2, 3, 4), result);
    }

    private static JobDiagnosticsService CreateService(
        IMonitoringJobRepository monitoringRepository,
        IJvCalculationRepository jvRepository,
        IReplayFlowRepository replayRepository,
        MonitoringJobsOptions? monitoringOptions = null,
        JvCalculationOptions? jvOptions = null,
        ReplayFlowsOptions? replayOptions = null)
    {
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider
            .Setup(provider => provider.GetService(typeof(IMonitoringJobRepository)))
            .Returns(monitoringRepository);
        serviceProvider
            .Setup(provider => provider.GetService(typeof(IJvCalculationRepository)))
            .Returns(jvRepository);
        serviceProvider
            .Setup(provider => provider.GetService(typeof(IReplayFlowRepository)))
            .Returns(replayRepository);

        var scope = new Mock<IServiceScope>();
        scope.SetupGet(value => value.ServiceProvider).Returns(serviceProvider.Object);

        var scopeFactory = new Mock<IServiceScopeFactory>();
        scopeFactory.Setup(factory => factory.CreateScope()).Returns(scope.Object);

        return new JobDiagnosticsService(
            scopeFactory.Object,
            Microsoft.Extensions.Options.Options.Create(monitoringOptions ?? new MonitoringJobsOptions()),
            Microsoft.Extensions.Options.Options.Create(jvOptions ?? new JvCalculationOptions()),
            Microsoft.Extensions.Options.Options.Create(replayOptions ?? new ReplayFlowsOptions()));
    }

    private static StuckReplayBatchRow MakeReplayRow(int ageSeconds)
    {
        return new StuckReplayBatchRow(
            FlowId: 1,
            FlowIdDerivedFrom: 2,
            PnlDate: new DateOnly(2026, 4, 24),
            PackageGuid: Guid.NewGuid(),
            CreatedBy: "user",
            DateCreated: DateTime.UtcNow.AddMinutes(-30),
            DateStarted: DateTime.UtcNow.AddSeconds(-ageSeconds),
            DateCompleted: null,
            ReplayStatus: "InProgress",
            ProcessStatus: "processing",
            AgeSeconds: ageSeconds);
    }
}