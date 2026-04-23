using Microsoft.Extensions.DependencyInjection;
using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class BackgroundJobCancellationServiceTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    [Fact]
    public async Task CancelMonitoringJobAsync_WhenStatusTransitionsToFailed_ReturnsConfirmed()
    {
        var cancellationRegistry = new JobCancellationRegistry();
        using var registeredToken = new CancellationTokenSource();
        cancellationRegistry.RegisterMonitoringJob(1L, registeredToken);

        var repository = new Mock<IMonitoringJobRepository>();
        repository
            .SetupSequence(current => current.GetMonitoringJobByIdAsync(1L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateMonitoringJob("Running"))
            .ReturnsAsync(CreateMonitoringJob("Failed"));
        repository
            .Setup(current => current.MarkMonitoringJobFailedAsync(1L, BackgroundJobCancellationService.MonitoringJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var service = CreateService(repository.Object, Mock.Of<IJvCalculationRepository>(), cancellationRegistry);

        var result = await service.CancelMonitoringJobAsync(1L, CancellationToken.None);

        Assert.True(result.WasActive);
        Assert.True(result.CancellationConfirmed);
        Assert.True(registeredToken.IsCancellationRequested);
    }

    [Fact]
    public async Task CancelMonitoringJobAsync_WhenStatusStaysActive_ReturnsPending()
    {
        var repository = new Mock<IMonitoringJobRepository>();
        repository
            .Setup(current => current.GetMonitoringJobByIdAsync(1L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateMonitoringJob("Running"));
        repository
            .Setup(current => current.MarkMonitoringJobFailedAsync(1L, BackgroundJobCancellationService.MonitoringJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var service = CreateService(repository.Object, Mock.Of<IJvCalculationRepository>());

        var result = await service.CancelMonitoringJobAsync(1L, CancellationToken.None);

        Assert.True(result.WasActive);
        Assert.False(result.CancellationConfirmed);
    }

    [Fact]
    public async Task CancelJvJobAsync_WhenStatusTransitionsToFailed_ReturnsConfirmed()
    {
        var cancellationRegistry = new JobCancellationRegistry();
        using var registeredToken = new CancellationTokenSource();
        cancellationRegistry.RegisterJvJob(1L, registeredToken);

        var repository = new Mock<IJvCalculationRepository>();
        repository
            .SetupSequence(current => current.GetJvJobByIdAsync(1L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateJvJob("Running"))
            .ReturnsAsync(CreateJvJob("Failed"));
        repository
            .Setup(current => current.MarkJvJobFailedAsync(1L, BackgroundJobCancellationService.JvJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var service = CreateService(Mock.Of<IMonitoringJobRepository>(), repository.Object, cancellationRegistry);

        var result = await service.CancelJvJobAsync(1L, CancellationToken.None);

        Assert.True(result.WasActive);
        Assert.True(result.CancellationConfirmed);
        Assert.True(registeredToken.IsCancellationRequested);
    }

    private static BackgroundJobCancellationService CreateService(
        IMonitoringJobRepository monitoringRepository,
        IJvCalculationRepository jvRepository,
        JobCancellationRegistry? cancellationRegistry = null)
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

        return new BackgroundJobCancellationService(scopeFactory.Object, cancellationRegistry ?? new JobCancellationRegistry());
    }

    private static MonitoringJobRecord CreateMonitoringJob(string status)
    {
        return new MonitoringJobRecord(
            JobId: 1L,
            Category: MonitoringJobHelper.DataValidationCategory,
            SubmenuKey: MonitoringJobHelper.BatchStatusSubmenuKey,
            DisplayName: "Batch Status",
            PnlDate: TestDate,
            Status: status,
            WorkerId: null,
            ParametersJson: null,
            ParameterSummary: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: DateTime.UtcNow,
            LastHeartbeatAt: DateTime.UtcNow,
            CompletedAt: status == "Completed" ? DateTime.UtcNow : null,
            FailedAt: status == "Failed" ? DateTime.UtcNow : null,
            ErrorMessage: status == "Failed" ? BackgroundJobCancellationService.MonitoringJobCanceledMessage : null,
            ParsedQuery: null,
            GridColumnsJson: null,
            GridRowsJson: null,
            MetadataJson: null,
            SavedAt: null);
    }

    private static JvJobRecord CreateJvJob(string status)
    {
        return new JvJobRecord(
            JobId: 1L,
            UserId: "user",
            PnlDate: TestDate,
            RequestType: "CheckOnly",
            Status: status,
            WorkerId: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: DateTime.UtcNow,
            LastHeartbeatAt: DateTime.UtcNow,
            CompletedAt: status == "Completed" ? DateTime.UtcNow : null,
            FailedAt: status == "Failed" ? DateTime.UtcNow : null,
            ErrorMessage: status == "Failed" ? BackgroundJobCancellationService.JvJobCanceledMessage : null,
            QueryCheck: null,
            QueryFix: null,
            GridColumnsJson: null,
            GridRowsJson: null,
            SavedAt: null);
    }
}