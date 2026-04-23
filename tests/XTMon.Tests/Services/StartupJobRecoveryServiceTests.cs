using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
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

        var service = CreateService(monitoringRepository.Object, jvRepository.Object);

        await service.StartAsync(CancellationToken.None);

        monitoringRepository.Verify(
            repository => repository.FailRunningMonitoringJobsAsync(StartupJobRecoveryService.MonitoringStartupRecoveryMessage, It.IsAny<CancellationToken>()),
            Times.Once);
        jvRepository.Verify(
            repository => repository.FailRunningJvJobsAsync(StartupJobRecoveryService.JvStartupRecoveryMessage, It.IsAny<CancellationToken>()),
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

        var service = CreateService(monitoringRepository.Object, jvRepository.Object);

        await service.StartAsync(CancellationToken.None);
    }

    private static StartupJobRecoveryService CreateService(
        IMonitoringJobRepository monitoringRepository,
        IJvCalculationRepository jvRepository)
    {
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider
            .Setup(provider => provider.GetService(typeof(IMonitoringJobRepository)))
            .Returns(monitoringRepository);
        serviceProvider
            .Setup(provider => provider.GetService(typeof(IJvCalculationRepository)))
            .Returns(jvRepository);

        var scope = new Mock<IServiceScope>();
        scope.Setup(currentScope => currentScope.ServiceProvider).Returns(serviceProvider.Object);
        scope.Setup(currentScope => currentScope.Dispose());

        var scopeFactory = new Mock<IServiceScopeFactory>();
        scopeFactory.Setup(factory => factory.CreateScope()).Returns(scope.Object);

        return new StartupJobRecoveryService(scopeFactory.Object, NullLogger<StartupJobRecoveryService>.Instance);
    }
}