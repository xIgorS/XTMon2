using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XTMon.Models;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class StartupJobRecoveryServiceTests
{
    [Fact]
    public async Task StartAsync_FailsRunningMonitoringAndJvJobs()
    {
        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.FailRunningMonitoringJobsAsync(StartupJobRecoveryService.MonitoringStartupRecoveryMessage, It.IsAny<CancellationToken>()))
            .ReturnsAsync(2);

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.FailRunningJvJobsAsync(StartupJobRecoveryService.JvStartupRecoveryMessage, It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.FailRunningReplayBatchesAsync(StartupJobRecoveryService.ReplayStartupRecoveryMessage, It.IsAny<CancellationToken>()))
            .ReturnsAsync(4);

        var service = CreateService(monitoringRepository.Object, jvRepository.Object, replayRepository.Object);

        await service.StartAsync(CancellationToken.None);

        monitoringRepository.Verify(
            repository => repository.FailRunningMonitoringJobsAsync(StartupJobRecoveryService.MonitoringStartupRecoveryMessage, It.IsAny<CancellationToken>()),
            Times.Once);
        jvRepository.Verify(
            repository => repository.FailRunningJvJobsAsync(StartupJobRecoveryService.JvStartupRecoveryMessage, It.IsAny<CancellationToken>()),
            Times.Once);
        replayRepository.Verify(
            repository => repository.FailRunningReplayBatchesAsync(StartupJobRecoveryService.ReplayStartupRecoveryMessage, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StartAsync_WhenRecoveryFails_DoesNotThrow()
    {
        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.FailRunningMonitoringJobsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));

        var jvRepository = new Mock<IJvCalculationRepository>();
        var replayRepository = new Mock<IReplayFlowRepository>();

        var service = CreateService(monitoringRepository.Object, jvRepository.Object, replayRepository.Object);

        await service.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task RecoverAsync_ReturnsRecoveryCounts()
    {
        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.FailRunningMonitoringJobsAsync(StartupJobRecoveryService.MonitoringStartupRecoveryMessage, It.IsAny<CancellationToken>()))
            .ReturnsAsync(3);

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.FailRunningJvJobsAsync(StartupJobRecoveryService.JvStartupRecoveryMessage, It.IsAny<CancellationToken>()))
            .ReturnsAsync(2);

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.FailRunningReplayBatchesAsync(StartupJobRecoveryService.ReplayStartupRecoveryMessage, It.IsAny<CancellationToken>()))
            .ReturnsAsync(5);

        var service = CreateService(monitoringRepository.Object, jvRepository.Object, replayRepository.Object);

        var result = await service.RecoverAsync(CancellationToken.None);

        Assert.Equal(new StartupJobRecoveryResult(3, 2, 5), result);
    }

    [Fact]
    public async Task RecoverAsync_WhenReplayRecoveryFails_StillReturnsMonitoringAndJvCounts()
    {
        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.FailRunningMonitoringJobsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(7);

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.FailRunningJvJobsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(3);

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.FailRunningReplayBatchesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Replay SP missing"));

        var service = CreateService(monitoringRepository.Object, jvRepository.Object, replayRepository.Object);

        var result = await service.RecoverAsync(CancellationToken.None);

        Assert.Equal(new StartupJobRecoveryResult(7, 3, 0), result);
    }

    private static StartupJobRecoveryService CreateService(
        IMonitoringJobRepository monitoringRepository,
        IJvCalculationRepository jvRepository,
        IReplayFlowRepository replayRepository)
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
        scope.Setup(currentScope => currentScope.ServiceProvider).Returns(serviceProvider.Object);
        scope.Setup(currentScope => currentScope.Dispose());

        var scopeFactory = new Mock<IServiceScopeFactory>();
        scopeFactory.Setup(factory => factory.CreateScope()).Returns(scope.Object);

        return new StartupJobRecoveryService(scopeFactory.Object, NullLogger<StartupJobRecoveryService>.Instance);
    }
}