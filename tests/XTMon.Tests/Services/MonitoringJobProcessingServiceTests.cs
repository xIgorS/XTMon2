using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System.Reflection;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class MonitoringJobProcessingServiceTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    private static IOptions<MonitoringJobsOptions> CreateOptions(int maxConcurrentJobs = 1, Dictionary<string, int>? categoryMaxConcurrentJobs = null) =>
        Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions
        {
            MaxConcurrentJobs = maxConcurrentJobs,
            CategoryMaxConcurrentJobs = categoryMaxConcurrentJobs ?? new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase),
            JobRunningStaleTimeoutSeconds = 1800,
            JobPollIntervalSeconds = 1
        });

    private static MonitoringJobRecord MakeJob(string category, string submenuKey, long jobId = 1L) =>
        new(
            JobId: jobId,
            Category: category,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: TestDate,
            Status: "Queued",
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

    private static (MonitoringJobProcessingService service, Mock<IMonitoringJobRepository> repo)
        CreateService(
            Mock<IMonitoringJobRepository> repo,
            IEnumerable<IMonitoringJobExecutor> executors,
            IOptions<MonitoringJobsOptions>? options = null,
            TimeSpan? idleDelay = null,
            TimeSpan? heartbeatInterval = null,
            JobCancellationRegistry? jobCancellationRegistry = null)
    {
        var sp = new Mock<IServiceProvider>();
        sp.Setup(p => p.GetService(typeof(IMonitoringJobRepository))).Returns(repo.Object);
        sp.Setup(p => p.GetService(typeof(IEnumerable<IMonitoringJobExecutor>))).Returns(executors);

        var scope = new Mock<IServiceScope>();
        scope.Setup(s => s.ServiceProvider).Returns(sp.Object);
        scope.Setup(s => s.Dispose());

        var factory = new Mock<IServiceScopeFactory>();
        factory.Setup(f => f.CreateScope()).Returns(scope.Object);

        var service = new MonitoringJobProcessingService(
            factory.Object,
            options ?? CreateOptions(),
            NullLogger<MonitoringJobProcessingService>.Instance,
            idleDelay ?? TimeSpan.FromSeconds(5),
            heartbeatInterval,
            jobCancellationRegistry);

        return (service, repo);
    }

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
                Status = "Running",
                StartedAt = DateTime.UtcNow
            });
        return repo;
    }

    [Fact]
    public async Task MaxConcurrentJobsOne_PreservesSerializedExecution()
    {
        var firstStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstReleaseTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReleaseTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionCount = 0;

        var job1 = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var job2 = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 2L);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job1)
            .ReturnsAsync(job2)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (job, _) =>
            {
                if (job.JobId == 1L)
                {
                    firstStartedTcs.TrySetResult(true);
                    await firstReleaseTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                    return payload;
                }

                secondStartedTcs.TrySetResult(true);
                await secondReleaseTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            options: CreateOptions(maxConcurrentJobs: 1),
            idleDelay: TimeSpan.FromMilliseconds(10));

        await service.StartAsync(CancellationToken.None);

        await firstStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await AssertRemainsIncompleteAsync(secondStartedTcs.Task, TimeSpan.FromMilliseconds(150));

        firstReleaseTcs.TrySetResult(true);
        await secondStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        secondReleaseTcs.TrySetResult(true);

        await EventuallyAsync(() => Volatile.Read(ref completionCount) == 2);
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task MaxConcurrentJobsTwo_AllowsParallelExecution()
    {
        var bothStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseJobsTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionCount = 0;
        var startedCount = 0;

        var job1 = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var job2 = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 2L);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job1)
            .ReturnsAsync(job2)
            .ReturnsAsync((MonitoringJobRecord?)null)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (_, _) =>
            {
                if (Interlocked.Increment(ref startedCount) == 2)
                {
                    bothStartedTcs.TrySetResult(true);
                }

                await releaseJobsTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            options: CreateOptions(maxConcurrentJobs: 2),
            idleDelay: TimeSpan.FromMilliseconds(10));

        await service.StartAsync(CancellationToken.None);

        await bothStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        releaseJobsTcs.TrySetResult(true);

        await EventuallyAsync(() => Volatile.Read(ref completionCount) == 2);
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task MaxConcurrentJobsTwo_PrefersDifferentCategoryWhenOneIsAlreadyActive()
    {
        var dataValidationStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var functionalRejectionStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseJobsTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionCount = 0;
        var functionalRejectionClaimed = false;

        var dataValidationJob = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var functionalRejectionJob = MakeJob(MonitoringJobHelper.FunctionalRejectionCategory, "fr|1|SYS|DTM|CODE", 2L);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(dataValidationJob)
            .ReturnsAsync((MonitoringJobRecord?)null)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<IReadOnlyCollection<string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, IReadOnlyCollection<string>? excludedCategories, CancellationToken _) =>
            {
                if (!functionalRejectionClaimed
                    && excludedCategories is not null
                    && excludedCategories.Contains(MonitoringJobHelper.DataValidationCategory, StringComparer.Ordinal))
                {
                    functionalRejectionClaimed = true;
                    return functionalRejectionJob;
                }

                return null;
            });
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (job, _) =>
            {
                if (job.Category == MonitoringJobHelper.DataValidationCategory)
                {
                    dataValidationStartedTcs.TrySetResult(true);
                }

                if (job.Category == MonitoringJobHelper.FunctionalRejectionCategory)
                {
                    functionalRejectionStartedTcs.TrySetResult(true);
                }

                await releaseJobsTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            options: CreateOptions(maxConcurrentJobs: 2),
            idleDelay: TimeSpan.FromMilliseconds(10));

        await service.StartAsync(CancellationToken.None);

        await dataValidationStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await functionalRejectionStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        releaseJobsTcs.TrySetResult(true);

        await EventuallyAsync(() => Volatile.Read(ref completionCount) == 2);
        await service.StopAsync(CancellationToken.None);

        repo.Verify(
            r => r.TryTakeNextMonitoringJobAsync(
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<string>?>(excludedCategories =>
                    excludedCategories != null
                    && excludedCategories.Contains(MonitoringJobHelper.DataValidationCategory)),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task CategoryLimitOne_PreventsClaimingSecondJobFromSameCategory()
    {
        var dataValidationStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var functionalRejectionStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseJobsTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionCount = 0;
        var secondDataValidationStarted = 0;

        var dataValidationJob = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var secondDataValidationJob = MakeJob(MonitoringJobHelper.DataValidationCategory, "daily-balance", 2L);
        var functionalRejectionJob = MakeJob(MonitoringJobHelper.FunctionalRejectionCategory, "fr|1|SYS|DTM|CODE", 3L);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var functionalRejectionClaimed = false;

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(dataValidationJob)
            .ReturnsAsync(secondDataValidationJob)
            .ReturnsAsync((MonitoringJobRecord?)null)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<IReadOnlyCollection<string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string _, IReadOnlyCollection<string>? excludedCategories, CancellationToken _) =>
            {
                if (!functionalRejectionClaimed
                    && excludedCategories != null
                    && excludedCategories.Contains(MonitoringJobHelper.DataValidationCategory))
                {
                    functionalRejectionClaimed = true;
                    return functionalRejectionJob;
                }

                return null;
            });
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (job, _) =>
            {
                if (job.JobId == dataValidationJob.JobId)
                {
                    dataValidationStartedTcs.TrySetResult(true);
                }

                if (job.JobId == functionalRejectionJob.JobId)
                {
                    functionalRejectionStartedTcs.TrySetResult(true);
                }

                if (job.JobId == secondDataValidationJob.JobId)
                {
                    Interlocked.Increment(ref secondDataValidationStarted);
                }

                await releaseJobsTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            options: CreateOptions(
                maxConcurrentJobs: 2,
                categoryMaxConcurrentJobs: new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
                {
                    [MonitoringJobHelper.DataValidationCategory] = 1,
                    [MonitoringJobHelper.FunctionalRejectionCategory] = 1
                }),
            idleDelay: TimeSpan.FromMilliseconds(10));

        await service.StartAsync(CancellationToken.None);

        await dataValidationStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await functionalRejectionStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.True(functionalRejectionClaimed);
    Assert.Equal(0, Volatile.Read(ref secondDataValidationStarted));

        releaseJobsTcs.TrySetResult(true);

    await EventuallyAsync(() => Volatile.Read(ref completionCount) >= 2);
        await service.StopAsync(CancellationToken.None);

        repo.Verify(
            r => r.TryTakeNextMonitoringJobAsync(
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<string>?>(excludedCategories =>
                    excludedCategories != null
                    && excludedCategories.Contains(MonitoringJobHelper.DataValidationCategory)),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task MatchingExecutor_SavesResultAndMarksCompleted()
    {
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            candidate => candidate.SubmenuKey == MonitoringJobHelper.BatchStatusSubmenuKey,
            (_, _) => Task.FromResult(payload));

        var (service, _) = CreateService(repo, [executor]);
        await service.StartAsync(CancellationToken.None);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.SaveMonitoringJobResultAsync(1L, payload, It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task WhenExecutorThrows_CallsMarkFailed()
    {
        var failedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob(MonitoringJobHelper.FunctionalRejectionCategory, "fr|1|SYS|DTM|CODE");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.MarkMonitoringJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => failedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            candidate => candidate.JobId == 1L,
            (_, _) => throw new InvalidOperationException("boom"));

        var (service, _) = CreateService(repo, [executor]);
        await service.StartAsync(CancellationToken.None);

        await failedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkMonitoringJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task OnEachPoll_CallsExpireStaleRunningJobs()
    {
        var twoExpiresTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.Setup(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                if (repo.Invocations.Count(invocation => invocation.Method.Name == nameof(IMonitoringJobRepository.ExpireStaleRunningMonitoringJobsAsync)) >= 2)
                {
                    twoExpiresTcs.TrySetResult(true);
                }

                return 0;
            });

        var (service, _) = CreateService(repo, [], idleDelay: TimeSpan.FromMilliseconds(10));
        await service.StartAsync(CancellationToken.None);

        await twoExpiresTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(
            r => r.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(2));
    }

    [Fact]
    public async Task WhenExpireStaleThrows_ServiceContinuesPolling()
    {
        var recoveredTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var expireCalls = 0;

        var repo = BaseRepo();
        repo.Setup(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns<TimeSpan, string, CancellationToken>((_, _, _) =>
            {
                expireCalls++;
                if (expireCalls == 1)
                {
                    throw new TimeoutException("stale expiry timed out");
                }

                recoveredTcs.TrySetResult(true);
                return Task.FromResult(0);
            });

        var (service, _) = CreateService(repo, [], idleDelay: TimeSpan.FromMilliseconds(10));
        await service.StartAsync(CancellationToken.None);

        await recoveredTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(
            r => r.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(2));
    }

    [Fact]
    public async Task WhenTakeNextThrows_ServiceContinuesPolling()
    {
        var recoveredTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var takeNextCalls = 0;

        var repo = BaseRepo();
        repo.Setup(r => r.ExpireStaleRunningMonitoringJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);
        repo.Setup(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns<string, CancellationToken>((_, _) =>
            {
                takeNextCalls++;
                if (takeNextCalls == 1)
                {
                    throw new TimeoutException("take next timed out");
                }

                recoveredTcs.TrySetResult(true);
                return Task.FromResult<MonitoringJobRecord?>(null);
            });

        var (service, _) = CreateService(repo, [], idleDelay: TimeSpan.FromMilliseconds(10));
        await service.StartAsync(CancellationToken.None);

        await recoveredTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(
            r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(2));
    }

    [Fact]
    public async Task LongRunningJob_SendsPeriodicHeartbeatsWhileExecuting()
    {
        var heartbeatCount = 0;
        var repeatedHeartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() =>
            {
                heartbeatCount++;
                if (heartbeatCount >= 3)
                {
                    repeatedHeartbeatTcs.TrySetResult(true);
                }
            })
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (_, _) =>
            {
                await repeatedHeartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(20));

        await service.StartAsync(CancellationToken.None);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.HeartbeatMonitoringJobAsync(1L, It.IsAny<CancellationToken>()), Times.AtLeast(3));
    }

    [Fact]
    public async Task LongRunningJob_HeartbeatLoopUsesSeparateScope()
    {
        var heartbeatCount = 0;
        var repeatedHeartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseJobTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var createdScopeCount = 0;
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey);
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() =>
            {
                heartbeatCount++;
                if (heartbeatCount >= 3)
                {
                    repeatedHeartbeatTcs.TrySetResult(true);
                }
            })
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (_, _) =>
            {
                await releaseJobTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var sp = new Mock<IServiceProvider>();
        sp.Setup(p => p.GetService(typeof(IMonitoringJobRepository))).Returns(repo.Object);
        sp.Setup(p => p.GetService(typeof(IEnumerable<IMonitoringJobExecutor>))).Returns(new[] { executor });

        var scope = new Mock<IServiceScope>();
        scope.Setup(s => s.ServiceProvider).Returns(sp.Object);
        scope.Setup(s => s.Dispose());

        var factory = new Mock<IServiceScopeFactory>();
        factory.Setup(f => f.CreateScope())
            .Callback(() => Interlocked.Increment(ref createdScopeCount))
            .Returns(scope.Object);

        var service = new MonitoringJobProcessingService(
            factory.Object,
            CreateOptions(),
            NullLogger<MonitoringJobProcessingService>.Instance,
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(20));

        await service.StartAsync(CancellationToken.None);

        await repeatedHeartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(Volatile.Read(ref createdScopeCount) >= 3);

        releaseJobTcs.TrySetResult(true);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task LongRunningJob_WhenHeartbeatTimesOut_StopsWorkerAndMarksFailed()
    {
        var executorCancelledTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.SetupSequence(r => r.HeartbeatMonitoringJobAsync(1L, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .ThrowsAsync(MakeSqlException(-2, "Heartbeat timeout"));
        repo.Setup(r => r.MarkMonitoringJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => failedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (_, token) =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    executorCancelledTcs.TrySetResult(true);
                    throw;
                }

                return new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(20));

        await service.StartAsync(CancellationToken.None);

        await executorCancelledTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await failedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkMonitoringJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task WhenMonitoringJobIsCancelled_DoesNotMarkCompleted()
    {
        var heartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationRequestedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey);
        var registry = new JobCancellationRegistry();
        var isCancelled = false;

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.GetMonitoringJobByIdAsync(1L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => MakeJob(
                MonitoringJobHelper.DataValidationCategory,
                MonitoringJobHelper.BatchStatusSubmenuKey) with
            {
                Status = isCancelled ? MonitoringJobHelper.CancelledStatus : "Running",
                FailedAt = isCancelled ? DateTime.UtcNow : null,
                ErrorMessage = isCancelled ? BackgroundJobCancellationService.MonitoringJobCanceledMessage : null
            });
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => heartbeatTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCancelledAsync(1L, BackgroundJobCancellationService.MonitoringJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (_, token) =>
            {
                await heartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                isCancelled = true;
                registry.CancelMonitoringJob(1L);
                cancellationRequestedTcs.TrySetResult(true);
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new MonitoringJobResultPayload(null, null, null);
            });

        var (service, _) = CreateService(repo, [executor], idleDelay: TimeSpan.FromMilliseconds(10), jobCancellationRegistry: registry);

        await service.StartAsync(CancellationToken.None);

        await cancellationRequestedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task WhenMonitoringJobIsCancelled_NextQueuedJobWaitsUntilCancelledTaskUnwinds()
    {
        var firstJobHeartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstJobCancelledTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondJobStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCancelledJobToFinishTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSecondJobTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstJob = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var secondJob = MakeJob(MonitoringJobHelper.DataValidationCategory, "daily-balance", 2L);
        var registry = new JobCancellationRegistry();
        var cancelledJobIds = new HashSet<long>();
        var completionCount = 0;

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(firstJob)
            .ReturnsAsync(secondJob)
            .ReturnsAsync((MonitoringJobRecord?)null)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.GetMonitoringJobByIdAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long jobId, CancellationToken _) => MakeJob(
                MonitoringJobHelper.DataValidationCategory,
                jobId == firstJob.JobId ? firstJob.SubmenuKey : secondJob.SubmenuKey,
                jobId) with
            {
                Status = cancelledJobIds.Contains(jobId) ? MonitoringJobHelper.CancelledStatus : "Running",
                StartedAt = DateTime.UtcNow,
                FailedAt = cancelledJobIds.Contains(jobId) ? DateTime.UtcNow : null,
                ErrorMessage = cancelledJobIds.Contains(jobId)
                    ? BackgroundJobCancellationService.MonitoringJobCanceledMessage
                    : null
            });
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(firstJob.JobId, It.IsAny<CancellationToken>()))
            .Callback(() => firstJobHeartbeatTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(secondJob.JobId, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(secondJob.JobId, It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCancelledAsync(firstJob.JobId, BackgroundJobCancellationService.MonitoringJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(
            _ => true,
            async (job, token) =>
            {
                if (job.JobId == firstJob.JobId)
                {
                    await firstJobHeartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                    cancelledJobIds.Add(firstJob.JobId);
                    registry.CancelMonitoringJob(firstJob.JobId);
                    firstJobCancelledTcs.TrySetResult(true);

                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        await allowCancelledJobToFinishTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                        throw;
                    }

                    return new MonitoringJobResultPayload(null, null, null);
                }

                secondJobStartedTcs.TrySetResult(true);
                await releaseSecondJobTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            options: CreateOptions(maxConcurrentJobs: 1),
            idleDelay: TimeSpan.FromMilliseconds(10),
            jobCancellationRegistry: registry);

        await service.StartAsync(CancellationToken.None);

        try
        {
            await firstJobCancelledTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await AssertRemainsIncompleteAsync(
                secondJobStartedTcs.Task,
                TimeSpan.FromMilliseconds(250),
                "The next queued job must not start while the cancelled task is still unwinding.");

            allowCancelledJobToFinishTcs.TrySetResult(true);

            await secondJobStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            releaseSecondJobTcs.TrySetResult(true);

            await EventuallyAsync(() => Volatile.Read(ref completionCount) == 1);
        }
        finally
        {
            releaseSecondJobTcs.TrySetResult(true);
            allowCancelledJobToFinishTcs.TrySetResult(true);
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task DataValidationCategoryLimitTwo_CancelledJobStillOccupiesSlotUntilTaskUnwinds()
    {
        var firstJobHeartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstJobCancelledTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondJobStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdJobStartedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCancelledJobToFinishTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRunningJobsTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstJob = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 1L);
        var secondJob = MakeJob(MonitoringJobHelper.DataValidationCategory, "daily-balance", 2L);
        var thirdJob = MakeJob(MonitoringJobHelper.DataValidationCategory, "pricing", 3L);
        var registry = new JobCancellationRegistry();
        var cancelledJobIds = new HashSet<long>();
        var completionCount = 0;

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(firstJob)
            .ReturnsAsync(secondJob)
            .ReturnsAsync(thirdJob)
            .ReturnsAsync((MonitoringJobRecord?)null)
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<IReadOnlyCollection<string>?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MonitoringJobRecord?)null);
        repo.Setup(r => r.GetMonitoringJobByIdAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long jobId, CancellationToken _) => MakeJob(
                MonitoringJobHelper.DataValidationCategory,
                jobId switch
                {
                    1L => firstJob.SubmenuKey,
                    2L => secondJob.SubmenuKey,
                    _ => thirdJob.SubmenuKey
                },
                jobId) with
            {
                Status = cancelledJobIds.Contains(jobId) ? MonitoringJobHelper.CancelledStatus : "Running",
                StartedAt = DateTime.UtcNow,
                FailedAt = cancelledJobIds.Contains(jobId) ? DateTime.UtcNow : null,
                ErrorMessage = cancelledJobIds.Contains(jobId)
                    ? BackgroundJobCancellationService.MonitoringJobCanceledMessage
                    : null
            });
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(firstJob.JobId, It.IsAny<CancellationToken>()))
            .Callback(() => firstJobHeartbeatTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(secondJob.JobId, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(thirdJob.JobId, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(It.Is<long>(jobId => jobId == secondJob.JobId || jobId == thirdJob.JobId), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref completionCount))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobCancelledAsync(firstJob.JobId, BackgroundJobCancellationService.MonitoringJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var executor = new StubExecutor(
            _ => true,
            async (job, token) =>
            {
                if (job.JobId == firstJob.JobId)
                {
                    await firstJobHeartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                    cancelledJobIds.Add(firstJob.JobId);
                    registry.CancelMonitoringJob(firstJob.JobId);
                    firstJobCancelledTcs.TrySetResult(true);

                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        await allowCancelledJobToFinishTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                        throw;
                    }

                    return payload;
                }

                if (job.JobId == secondJob.JobId)
                {
                    secondJobStartedTcs.TrySetResult(true);
                }

                if (job.JobId == thirdJob.JobId)
                {
                    thirdJobStartedTcs.TrySetResult(true);
                }

                await releaseRunningJobsTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                return payload;
            });

        var (service, _) = CreateService(
            repo,
            [executor],
            options: CreateOptions(
                maxConcurrentJobs: 2,
                categoryMaxConcurrentJobs: new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
                {
                    [MonitoringJobHelper.DataValidationCategory] = 2
                }),
            idleDelay: TimeSpan.FromMilliseconds(10),
            jobCancellationRegistry: registry);

        await service.StartAsync(CancellationToken.None);

        try
        {
            await secondJobStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await firstJobCancelledTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await AssertRemainsIncompleteAsync(
                thirdJobStartedTcs.Task,
                TimeSpan.FromMilliseconds(250),
                "A cancelled Data Validation task must still count toward the category limit until it fully unwinds.");

            allowCancelledJobToFinishTcs.TrySetResult(true);

            await thirdJobStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            releaseRunningJobsTcs.TrySetResult(true);
            await EventuallyAsync(() => Volatile.Read(ref completionCount) == 2);
        }
        finally
        {
            releaseRunningJobsTcs.TrySetResult(true);
            allowCancelledJobToFinishTcs.TrySetResult(true);
            await service.StopAsync(CancellationToken.None);
        }

        repo.Verify(
            r => r.TryTakeNextMonitoringJobAsync(
                It.IsAny<string>(),
                It.Is<IReadOnlyCollection<string>?>(excludedCategories =>
                    excludedCategories != null
                    && excludedCategories.Contains(MonitoringJobHelper.DataValidationCategory)),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
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

    [Fact]
    public async Task MarkCompleted_RetriesOnTransientFailure_AndEventuallySucceeds()
    {
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 42L);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);

        var markCompletedCalls = 0;
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var call = Interlocked.Increment(ref markCompletedCalls);
                if (call == 1)
                {
                    throw new InvalidOperationException("transient sql blip");
                }
                completedTcs.TrySetResult(true);
                return Task.CompletedTask;
            });

        var executor = new StubExecutor(_ => true, (_, _) => Task.FromResult(payload));

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10));

        await service.StartAsync(CancellationToken.None);
        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None);

        // Retry helper: first attempt throws, second succeeds. No mark-failed should be attempted.
        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()), Times.AtLeast(2));
        repo.Verify(r => r.MarkMonitoringJobFailedAsync(job.JobId, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SingleHeartbeatTimeout_DoesNotCancelExecution()
    {
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 101L);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);

        // Heartbeat sequence: first in-line HeartbeatMonitoringJobAsync at the top of ProcessJobAsync
        // must succeed; subsequent heartbeat loop call fails once with a SqlException timeout
        // (would have been "fatal" under the old code) but the execution must still complete.
        var heartbeatCall = 0;
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var n = Interlocked.Increment(ref heartbeatCall);
                if (n == 2)
                {
                    throw MakeSqlException(-2, "timeout");
                }
                return Task.CompletedTask;
            });
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executionStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var executionRelease = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = new StubExecutor(_ => true, async (_, token) =>
        {
            executionStarted.TrySetResult(true);
            await executionRelease.Task.WaitAsync(token);
            return payload;
        });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(50));

        await service.StartAsync(CancellationToken.None);
        await executionStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Give the heartbeat loop time to emit at least one failing tick.
        await AssertRemainsIncompleteAsync(
            completedTcs.Task,
            TimeSpan.FromMilliseconds(200),
            "Execution should still be running; a single heartbeat failure must not cancel it.");

        executionRelease.TrySetResult(true);
        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ThreeConsecutiveHeartbeatFailures_MarksJobFailed()
    {
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 202L);
        var markFailedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);

        // First heartbeat call (pre-loop) succeeds; every subsequent heartbeat in the loop fails.
        var heartbeatCall = 0;
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var n = Interlocked.Increment(ref heartbeatCall);
                if (n == 1)
                {
                    return Task.CompletedTask;
                }
                throw new InvalidOperationException("persistent heartbeat failure");
            });
        repo.Setup(r => r.MarkMonitoringJobFailedAsync(job.JobId, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => markFailedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executionRelease = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = new StubExecutor(_ => true, async (_, token) =>
        {
            try
            {
                // Block so we give the heartbeat loop time to fail 3× before returning.
                await Task.Delay(TimeSpan.FromSeconds(30), token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                throw;
            }

            return payload;
        });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(40));

        await service.StartAsync(CancellationToken.None);
        await markFailedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        executionRelease.TrySetResult(true);
        await service.StopAsync(CancellationToken.None);

        // 3 loop-heartbeat failures required, plus the 1 initial pre-loop heartbeat success = 4.
        repo.Verify(r => r.HeartbeatMonitoringJobAsync(job.JobId, It.IsAny<CancellationToken>()), Times.AtLeast(4));
        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task TwoConsecutiveTransientHeartbeatSqlFailures_DoNotMarkJobFailed()
    {
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 212L);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var recoveredHeartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);

        var heartbeatCall = 0;
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var n = Interlocked.Increment(ref heartbeatCall);
                if (n == 1)
                {
                    return Task.CompletedTask;
                }

                if (n is 2 or 3)
                {
                    throw MakeSqlException(1222, "lock timeout");
                }

                recoveredHeartbeatTcs.TrySetResult(true);
                return Task.CompletedTask;
            });
        repo.Setup(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(_ => true, async (_, token) =>
        {
            await recoveredHeartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5), token);
            return payload;
        });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(40));

        await service.StartAsync(CancellationToken.None);
        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkMonitoringJobFailedAsync(job.JobId, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ThreeConsecutiveTransientHeartbeatSqlFailures_MarksJobFailed()
    {
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 213L);
        var markFailedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);

        var heartbeatCall = 0;
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(job.JobId, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var n = Interlocked.Increment(ref heartbeatCall);
                if (n == 1)
                {
                    return Task.CompletedTask;
                }

                throw MakeSqlException(1222, "lock timeout");
            });
        repo.Setup(r => r.MarkMonitoringJobFailedAsync(job.JobId, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => markFailedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var executor = new StubExecutor(_ => true, async (_, token) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(30), token);
            return payload;
        });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(40));

        await service.StartAsync(CancellationToken.None);
        await markFailedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkMonitoringJobFailedAsync(job.JobId, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(job.JobId, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HeartbeatObservesDbCancel_CancelsExecution()
    {
        var payload = new MonitoringJobResultPayload("SELECT 1", new MonitoringTableResult([], []), null);
        var job = MakeJob(MonitoringJobHelper.DataValidationCategory, MonitoringJobHelper.BatchStatusSubmenuKey, 303L);
        var executionCancelledTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextMonitoringJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((MonitoringJobRecord?)null);

        // GetMonitoringJobByIdAsync: early calls return Running (two pre-heartbeat-loop calls happen
        // at L242 and L251); later calls (inside the heartbeat loop) return Cancelled, simulating an
        // out-of-band cancel (e.g. from another worker or BackgroundJobCancellationService).
        var getByIdCallCount = 0;
        repo.Setup(r => r.GetMonitoringJobByIdAsync(job.JobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                var call = Interlocked.Increment(ref getByIdCallCount);
                var status = call >= 4 ? MonitoringJobHelper.CancelledStatus : "Running";
                return MakeJob(job.Category, job.SubmenuKey, job.JobId) with
                {
                    Status = status,
                    StartedAt = DateTime.UtcNow,
                    FailedAt = status == MonitoringJobHelper.CancelledStatus ? DateTime.UtcNow : null
                };
            });

        var executor = new StubExecutor(_ => true, async (_, token) =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                executionCancelledTcs.TrySetResult(true);
                throw;
            }

            return payload;
        });

        var (service, _) = CreateService(
            repo,
            [executor],
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(40));

        await service.StartAsync(CancellationToken.None);

        // Executor should get its cancellation token fired once the heartbeat loop observes
        // the DB-side Cancelled flip — that's the whole point of the mid-run DB-cancel observation.
        await executionCancelledTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StopHeartbeatLoopAsync_WhenCalledTwice_DoesNotThrow()
    {
        var method = typeof(MonitoringJobProcessingService).GetMethod(
            "StopHeartbeatLoopAsync",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(method);

        var heartbeatLoopCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        var heartbeatLoopTask = Task.CompletedTask;
        var logger = NullLogger.Instance;

        var firstStop = (Task)method!.Invoke(null, [heartbeatLoopCts, heartbeatLoopTask, logger, 1L, "test processor"])!;
        await firstStop;

        var secondStop = (Task)method.Invoke(null, [heartbeatLoopCts, heartbeatLoopTask, logger, 1L, "test processor"])!;
        await secondStop;
    }

    private static SqlException MakeSqlException(int number, string message)
    {
        const BindingFlags NonPublicInstance = BindingFlags.NonPublic | BindingFlags.Instance;

        var errorCtor = typeof(SqlError)
            .GetConstructors(NonPublicInstance)
            .OrderByDescending(constructor => constructor.GetParameters().Length)
            .First();

        var errorArgs = errorCtor.GetParameters()
            .Select(parameter => parameter.ParameterType.IsValueType ? Activator.CreateInstance(parameter.ParameterType) : null)
            .ToArray();

        for (var index = 0; index < errorCtor.GetParameters().Length; index++)
        {
            var parameter = errorCtor.GetParameters()[index];
            if (parameter.ParameterType == typeof(int) && string.Equals(parameter.Name, "infoNumber", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = number;
            }
            else if (parameter.ParameterType == typeof(byte) && string.Equals(parameter.Name, "errorClass", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = (byte)16;
            }
            else if (parameter.ParameterType == typeof(byte) && string.Equals(parameter.Name, "state", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = (byte)1;
            }
            else if (parameter.ParameterType == typeof(string) && string.Equals(parameter.Name, "server", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = "server";
            }
            else if (parameter.ParameterType == typeof(string) && string.Equals(parameter.Name, "message", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = message;
            }
            else if (parameter.ParameterType == typeof(string) && string.Equals(parameter.Name, "procedure", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = "procedure";
            }
            else if (parameter.ParameterType == typeof(int) && string.Equals(parameter.Name, "lineNumber", StringComparison.OrdinalIgnoreCase))
            {
                errorArgs[index] = 1;
            }
        }

        var error = (SqlError)errorCtor.Invoke(errorArgs);

        var collectionCtor = typeof(SqlErrorCollection).GetConstructors(NonPublicInstance).First();
        var errors = (SqlErrorCollection)collectionCtor.Invoke(null);
        var addMethod = typeof(SqlErrorCollection).GetMethod("Add", NonPublicInstance)!;
        addMethod.Invoke(errors, [error]);

        var exceptionCtor = typeof(SqlException).GetConstructors(NonPublicInstance).First();
        var exceptionArgs = exceptionCtor.GetParameters()
            .Select(parameter => parameter.ParameterType.IsValueType ? Activator.CreateInstance(parameter.ParameterType) : null)
            .ToArray();

        for (var index = 0; index < exceptionCtor.GetParameters().Length; index++)
        {
            var parameter = exceptionCtor.GetParameters()[index];
            if (parameter.ParameterType == typeof(string))
            {
                exceptionArgs[index] = message;
            }
            else if (parameter.ParameterType == typeof(SqlErrorCollection))
            {
                exceptionArgs[index] = errors;
            }
            else if (parameter.ParameterType == typeof(Exception))
            {
                exceptionArgs[index] = null;
            }
            else if (parameter.ParameterType.FullName == "System.Guid")
            {
                exceptionArgs[index] = Guid.NewGuid();
            }
        }

        return (SqlException)exceptionCtor.Invoke(exceptionArgs);
    }

    private sealed class StubExecutor : IMonitoringJobExecutor
    {
        private readonly Func<MonitoringJobRecord, bool> _predicate;
        private readonly Func<MonitoringJobRecord, CancellationToken, Task<MonitoringJobResultPayload>> _callback;

        public StubExecutor(
            Func<MonitoringJobRecord, bool> predicate,
            Func<MonitoringJobRecord, CancellationToken, Task<MonitoringJobResultPayload>> callback)
        {
            _predicate = predicate;
            _callback = callback;
        }

        public bool CanExecute(MonitoringJobRecord job)
        {
            return _predicate(job);
        }

        public Task<MonitoringJobResultPayload> ExecuteAsync(MonitoringJobRecord job, CancellationToken cancellationToken)
        {
            return _callback(job, cancellationToken);
        }
    }
}