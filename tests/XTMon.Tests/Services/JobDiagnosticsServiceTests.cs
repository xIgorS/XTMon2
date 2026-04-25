using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using XTMon.Helpers;
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
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HashSet<long>());

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
    public async Task GetStuckJobsReportAsync_ExcludesMonitoringJobsWithActiveDmvRuntime()
    {
        var activeRuntimeJob = MakeMonitoringJob(1L, "batch-status");
        var orphanedJob = MakeMonitoringJob(2L, "daily-balance");

        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.GetStuckMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { activeRuntimeJob, orphanedJob });
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HashSet<long> { activeRuntimeJob.JobId });

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.GetStuckJvJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<JvJobRecord>());

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.GetStuckReplayBatchesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<StuckReplayBatchRow>());

        var service = CreateService(monitoringRepository.Object, jvRepository.Object, replayRepository.Object);

        var report = await service.GetStuckJobsReportAsync(CancellationToken.None);

        var job = Assert.Single(report.StuckMonitoringJobs);
        Assert.Equal(orphanedJob.JobId, job.JobId);
    }

    [Fact]
    public async Task GetStuckJobsReportAsync_WhenMonitoringDmvLookupFails_ReturnsStaleMonitoringRows()
    {
        var monitoringJob = MakeMonitoringJob(1L, "batch-status");

        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.GetStuckMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { monitoringJob });
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("DMV lookup failed"));

        var jvRepository = new Mock<IJvCalculationRepository>();
        jvRepository
            .Setup(repository => repository.GetStuckJvJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<JvJobRecord>());

        var replayRepository = new Mock<IReplayFlowRepository>();
        replayRepository
            .Setup(repository => repository.GetStuckReplayBatchesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<StuckReplayBatchRow>());

        var service = CreateService(monitoringRepository.Object, jvRepository.Object, replayRepository.Object);

        var report = await service.GetStuckJobsReportAsync(CancellationToken.None);

        var job = Assert.Single(report.StuckMonitoringJobs);
        Assert.Equal(monitoringJob.JobId, job.JobId);
    }

    [Fact]
    public async Task ForceExpireAllStuckAsync_UsesSameThresholdsAsStuckPanel()
    {
        var monitoringThreshold = TimeSpan.Zero;
        var jvThreshold = TimeSpan.Zero;
        var replayThreshold = TimeSpan.Zero;

        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HashSet<long>());
        monitoringRepository
            .Setup(repository => repository.RecoverOrphanedMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
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

    [Fact]
    public async Task GetMonitoringProcessorHealthReportAsync_FlagsUnderfilledProcessor()
    {
        var activeRunning = MakeMonitoringJob(1L, "batch-status");
        var queuedBacklog = MakeMonitoringJob(2L, "daily-balance") with
        {
            Status = MonitoringJobHelper.QueuedStatus,
            WorkerId = null,
            StartedAt = null,
            LastHeartbeatAt = null,
            EnqueuedAt = DateTime.UtcNow.AddSeconds(-30)
        };

        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.GetActiveMonitoringJobsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new[] { activeRunning, queuedBacklog });
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HashSet<long> { activeRunning.JobId });

        var service = CreateService(
            monitoringRepository.Object,
            Mock.Of<IJvCalculationRepository>(),
            Mock.Of<IReplayFlowRepository>(),
            monitoringOptions: new MonitoringJobsOptions
            {
                MaxConcurrentJobs = 3,
                ProcessorIdleDelaySeconds = 5,
                CategoryMaxConcurrentJobs = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
                {
                    [MonitoringJobHelper.DataValidationCategory] = 2,
                    [MonitoringJobHelper.FunctionalRejectionCategory] = 1
                }
            });

        var report = await service.GetMonitoringProcessorHealthReportAsync(CancellationToken.None);

        var dataValidationRow = Assert.Single(report.Rows, row => row.Category == MonitoringJobHelper.DataValidationCategory);
        Assert.True(dataValidationRow.HasIssue);
        Assert.Equal("Underfilled", dataValidationRow.Status);
        Assert.Equal(2, dataValidationRow.ConfiguredWorkers);
        Assert.Equal(1, dataValidationRow.LiveRuntimeJobs);
        Assert.Equal(1, dataValidationRow.QueuedJobs);
    }

    [Fact]
    public async Task GetMonitoringProcessorHealthReportAsync_WhenDmvFails_MarksRuntimeCheckUnavailable()
    {
        var monitoringRepository = new Mock<IMonitoringJobRepository>();
        monitoringRepository
            .Setup(repository => repository.GetActiveMonitoringJobsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<MonitoringJobRecord>());
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("DMV lookup failed"));

        var service = CreateService(
            monitoringRepository.Object,
            Mock.Of<IJvCalculationRepository>(),
            Mock.Of<IReplayFlowRepository>());

        var report = await service.GetMonitoringProcessorHealthReportAsync(CancellationToken.None);

        Assert.False(report.DmvAvailable);
        Assert.All(report.Rows, row => Assert.True(row.IsRuntimeCheckUnavailable));
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
            NullLogger<JobDiagnosticsService>.Instance,
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

    private static MonitoringJobRecord MakeMonitoringJob(long jobId, string submenuKey)
    {
        return new MonitoringJobRecord(
            JobId: jobId,
            Category: MonitoringJobHelper.DataValidationCategory,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: new DateOnly(2026, 4, 24),
            Status: "Running",
            WorkerId: "worker-1",
            ParametersJson: null,
            ParameterSummary: null,
            EnqueuedAt: DateTime.UtcNow.AddMinutes(-30),
            StartedAt: DateTime.UtcNow.AddMinutes(-25),
            LastHeartbeatAt: DateTime.UtcNow.AddMinutes(-20),
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