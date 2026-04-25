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

public class IndependentMonitoringJobProcessingServiceTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    private static IOptions<MonitoringJobsOptions> CreateOptions(int maxConcurrentJobs = 3, Dictionary<string, int>? categoryMaxConcurrentJobs = null) =>
        Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions
        {
            MaxConcurrentJobs = maxConcurrentJobs,
            CategoryMaxConcurrentJobs = categoryMaxConcurrentJobs ?? new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase),
            JobRunningStaleTimeoutSeconds = 1800,
            JobPollIntervalSeconds = 1
        });

    private static MonitoringJobRecord MakeJob(string category, string submenuKey, long jobId) =>
        new(
            JobId: jobId,
            Category: category,
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

    private static Mock<IMonitoringJobRepository> BaseRepo()
    {
        var repo = new Mock<IMonitoringJobRepository>();
        repo.Setup(r => r.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.SaveMonitoringJobResultAsync(It.IsAny<long>(), It.IsAny<MonitoringJobResultPayload>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobFailedAsync(It.IsAny<long>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.GetMonitoringJobByIdAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long jobId, CancellationToken _) => MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, jobId) with
            {
                Status = MonitoringJobHelper.RunningStatus,
                StartedAt = DateTime.UtcNow
            });
        return repo;
    }

    private static DataValidationMonitoringJobProcessingService CreateDataValidationService(
        Mock<IMonitoringJobRepository> repository,
        IEnumerable<IMonitoringJobExecutor> executors,
        IOptions<MonitoringJobsOptions>? options = null,
        TimeSpan? idleDelay = null,
        TimeSpan? heartbeatInterval = null,
        JobCancellationRegistry? jobCancellationRegistry = null)
    {
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider.Setup(provider => provider.GetService(typeof(IMonitoringJobRepository))).Returns(repository.Object);
        serviceProvider.Setup(provider => provider.GetService(typeof(IEnumerable<IMonitoringJobExecutor>))).Returns(executors);

        var scope = new Mock<IServiceScope>();
        scope.Setup(currentScope => currentScope.ServiceProvider).Returns(serviceProvider.Object);
        scope.Setup(currentScope => currentScope.Dispose());

        var scopeFactory = new Mock<IServiceScopeFactory>();
        scopeFactory.Setup(factory => factory.CreateScope()).Returns(scope.Object);

        return new DataValidationMonitoringJobProcessingService(
            scopeFactory.Object,
            options ?? CreateOptions(),
            NullLogger<DataValidationMonitoringJobProcessingService>.Instance,
            idleDelay ?? TimeSpan.FromMilliseconds(10),
            heartbeatInterval,
            jobCancellationRegistry);
    }

    [Fact]
    public async Task DataValidationProcessor_ClaimsOnlyOwnedCategory()
    {
        var startedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var claimedExcludedCategories = new List<string[]>();
        var claimCallCount = 0;
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repository = BaseRepo();
        repository
            .Setup(repo => repo.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<IReadOnlyCollection<string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, IReadOnlyCollection<string>? excludedCategories, CancellationToken _) =>
            {
                claimedExcludedCategories.Add(excludedCategories?.ToArray() ?? []);
                return Interlocked.Increment(ref claimCallCount) == 1
                    ? job
                    : null;
            });
        repository
            .Setup(repo => repo.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            candidate => string.Equals(candidate.Category, MonitoringJobHelper.DataValidationCategory, StringComparison.Ordinal),
            (_, _) =>
            {
                startedTcs.TrySetResult(true);
                return Task.FromResult(payload);
            });

        var service = CreateDataValidationService(repository, [executor]);

        await service.StartAsync(CancellationToken.None);
        await startedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repository.Verify(repo => repo.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Contains(claimedExcludedCategories, excludedCategories => excludedCategories.Contains(MonitoringJobHelper.FunctionalRejectionCategory));
        Assert.DoesNotContain(claimedExcludedCategories, excludedCategories => excludedCategories.Contains(MonitoringJobHelper.DataValidationCategory));
    }

    [Fact]
    public async Task DataValidationProcessor_UsesConfiguredWorkerLimit()
    {
        var firstJobStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondJobStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdJobStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseJobsTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job1 = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var job2 = MakeJob(MonitoringJobHelper.DataValidationCategory, "daily-balance", 2L);
        var job3 = MakeJob(MonitoringJobHelper.DataValidationCategory, "pricing", 3L);
        var completionCount = 0;
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repository = BaseRepo();
        repository
            .SetupSequence(repo => repo.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<IReadOnlyCollection<string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job1)
            .ReturnsAsync(job2)
            .ReturnsAsync(job3)
            .ReturnsAsync((MonitoringJobRecord?)null)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repository
            .Setup(repo => repo.MarkMonitoringJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            candidate => string.Equals(candidate.Category, MonitoringJobHelper.DataValidationCategory, StringComparison.Ordinal),
            async (job, _) =>
            {
                if (job.JobId == job1.JobId)
                {
                    firstJobStartedTcs.TrySetResult(true);
                }

                if (job.JobId == job2.JobId)
                {
                    secondJobStartedTcs.TrySetResult(true);
                }

                if (job.JobId == job3.JobId)
                {
                    thirdJobStartedTcs.TrySetResult(true);
                }

                await releaseJobsTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var service = CreateDataValidationService(
            repository,
            [executor],
            options: CreateOptions(
                maxConcurrentJobs: 3,
                categoryMaxConcurrentJobs: new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
                {
                    [MonitoringJobHelper.DataValidationCategory] = 2,
                    [MonitoringJobHelper.FunctionalRejectionCategory] = 1
                }));

        await service.StartAsync(CancellationToken.None);

        try
        {
            await firstJobStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await secondJobStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await AssertRemainsIncompleteAsync(
                thirdJobStartedTcs.Task,
                TimeSpan.FromMilliseconds(250),
                "The Data Validation processor should stop at its configured worker count of 2.");

            releaseJobsTcs.TrySetResult(true);
            await thirdJobStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await EventuallyAsync(() => Volatile.Read(ref completionCount) >= 3);
        }
        finally
        {
            releaseJobsTcs.TrySetResult(true);
            await service.StopAsync(CancellationToken.None);
        }
    }

    private static async Task EventuallyAsync(Func<bool> condition)
    {
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (!condition())
        {
            timeoutCts.Token.ThrowIfCancellationRequested();
            await Task.Delay(25, timeoutCts.Token);
        }
    }

    private static async Task AssertRemainsIncompleteAsync(Task task, TimeSpan duration, string? message = null)
    {
        using var timeoutCts = new CancellationTokenSource(duration);

        while (!timeoutCts.IsCancellationRequested)
        {
            Assert.False(task.IsCompleted, message);
            await Task.Delay(25, CancellationToken.None);
        }
    }

    private sealed class StubExecutor : IMonitoringJobExecutor
    {
        private readonly Func<MonitoringJobRecord, bool> _canExecute;
        private readonly Func<MonitoringJobRecord, CancellationToken, Task<MonitoringJobResultPayload>> _execute;

        public StubExecutor(
            Func<MonitoringJobRecord, bool> canExecute,
            Func<MonitoringJobRecord, CancellationToken, Task<MonitoringJobResultPayload>> execute)
        {
            _canExecute = canExecute;
            _execute = execute;
        }

        public bool CanExecute(MonitoringJobRecord job) => _canExecute(job);

        public Task<MonitoringJobResultPayload> ExecuteAsync(MonitoringJobRecord job, CancellationToken cancellationToken) =>
            _execute(job, cancellationToken);
    }
}