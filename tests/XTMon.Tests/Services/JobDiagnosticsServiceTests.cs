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
    public async Task GetMonitoringProcessorHealthReportAsync_SplitsQueuedRowsByProcessorScope()
    {
        var queuedPricing = MakeMonitoringJob(1L, MonitoringJobHelper.PricingSubmenuKey) with
        {
            Status = MonitoringJobHelper.QueuedStatus,
            WorkerId = null,
            StartedAt = null,
            LastHeartbeatAt = null,
            EnqueuedAt = DateTime.UtcNow.AddSeconds(-30)
        };
        var queuedBatchStatus = MakeMonitoringJob(2L, MonitoringJobHelper.BatchStatusSubmenuKey) with
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
            .ReturnsAsync(new[] { queuedPricing, queuedBatchStatus });
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HashSet<long>());

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

        var pricingRow = Assert.Single(report.Rows, row => row.ProcessorKey == $"{MonitoringJobHelper.DataValidationCategory}:{MonitoringJobHelper.PricingSubmenuKey}");
        var dataValidationRow = Assert.Single(report.Rows, row => row.ProcessorKey == MonitoringJobHelper.DataValidationCategory);

        Assert.Equal(1, pricingRow.QueuedJobs);
        Assert.Equal(0, pricingRow.RunningJobs);
        Assert.Equal(1, dataValidationRow.QueuedJobs);
    }

    [Fact]
    public async Task GetMonitoringProcessorHealthReportAsync_FlagsPricingProcessorUnderfilledWhenBacklogAgesPastGracePeriod()
    {
        var queuedPricing = MakeMonitoringJob(1L, MonitoringJobHelper.PricingSubmenuKey) with
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
            .ReturnsAsync(new[] { queuedPricing });
        monitoringRepository
            .Setup(repository => repository.GetRunningMonitoringJobIdsByDmvAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HashSet<long>());

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
                    [MonitoringJobHelper.DataValidationCategory] = 3,
                    [MonitoringJobHelper.FunctionalRejectionCategory] = 1
                }
            });

        var report = await service.GetMonitoringProcessorHealthReportAsync(CancellationToken.None);

        var pricingRow = Assert.Single(report.Rows, row => row.ProcessorKey == $"{MonitoringJobHelper.DataValidationCategory}:{MonitoringJobHelper.PricingSubmenuKey}");
        Assert.Equal(1, pricingRow.ConfiguredWorkers);
        Assert.Equal(0, pricingRow.LiveRuntimeJobs);
        Assert.Equal(1, pricingRow.QueuedJobs);
        Assert.True(pricingRow.HasIssue);
        Assert.Equal("Underfilled", pricingRow.Status);
    }

    [Fact]
    public async Task GetMonitoringProcessorHealthReportAsync_WhenDmvFails_MarksEveryRowUnavailable()
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
        Assert.All(report.Rows, row =>
        {
            Assert.True(row.IsRuntimeCheckUnavailable);
            Assert.Equal("DMV unavailable", row.Status);
        });
    }

    private static JobDiagnosticsService CreateService(
        IMonitoringJobRepository monitoringRepository,
        IJvCalculationRepository jvRepository,
        IReplayFlowRepository replayRepository,
        IReadOnlyCollection<IMonitoringJobProcessor>? processors = null,
        MonitoringJobsOptions? monitoringOptions = null,
        JvCalculationOptions? jvOptions = null,
        ReplayFlowsOptions? replayOptions = null)
    {
        var effectiveMonitoringOptions = monitoringOptions ?? new MonitoringJobsOptions();
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
            processors ?? CreateMonitoringProcessors(effectiveMonitoringOptions),
            Microsoft.Extensions.Options.Options.Create(effectiveMonitoringOptions),
            Microsoft.Extensions.Options.Options.Create(jvOptions ?? new JvCalculationOptions()),
            Microsoft.Extensions.Options.Options.Create(replayOptions ?? new ReplayFlowsOptions()));
    }

    private static IMonitoringJobProcessor[] CreateMonitoringProcessors(MonitoringJobsOptions options)
    {
        return
        [
            MakeProcessor(
                "Data Validation",
                MonitoringJobHelper.DataValidationCategory,
                [],
                [MonitoringJobHelper.PricingSubmenuKey, MonitoringJobHelper.DailyBalanceSubmenuKey],
                GetConfiguredWorkerCount(options, MonitoringJobHelper.DataValidationCategory)),
            MakeProcessor(
                "Pricing",
                MonitoringJobHelper.DataValidationCategory,
                [MonitoringJobHelper.PricingSubmenuKey],
                [],
                1),
            MakeProcessor(
                "Daily Balance",
                MonitoringJobHelper.DataValidationCategory,
                [MonitoringJobHelper.DailyBalanceSubmenuKey],
                [],
                1),
            MakeProcessor(
                "Functional Rejection",
                MonitoringJobHelper.FunctionalRejectionCategory,
                [],
                [],
                GetConfiguredWorkerCount(options, MonitoringJobHelper.FunctionalRejectionCategory))
        ];
    }

    private static IMonitoringJobProcessor MakeProcessor(
        string name,
        string category,
        IReadOnlyList<string> includedSubmenuKeys,
        IReadOnlyList<string> excludedSubmenuKeys,
        int maxConcurrentJobs)
    {
        return new TestMonitoringJobProcessor(
            new MonitoringProcessorIdentity(name, category, includedSubmenuKeys, excludedSubmenuKeys, maxConcurrentJobs));
    }

    private static int GetConfiguredWorkerCount(MonitoringJobsOptions options, string category)
    {
        return options.CategoryMaxConcurrentJobs.TryGetValue(category, out var configuredWorkers)
            ? configuredWorkers
            : options.MaxConcurrentJobs;
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

    private sealed record TestMonitoringJobProcessor(MonitoringProcessorIdentity Identity) : IMonitoringJobProcessor;
}