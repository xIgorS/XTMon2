using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class MonitoringJobProcessingServiceTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    private static IOptions<MonitoringJobsOptions> DefaultOptions() =>
        Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions
        {
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
            options ?? DefaultOptions(),
            NullLogger<MonitoringJobProcessingService>.Instance,
            idleDelay ?? TimeSpan.FromSeconds(5),
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
                Status = isCancelled ? "Failed" : "Running",
                FailedAt = isCancelled ? DateTime.UtcNow : null,
                ErrorMessage = isCancelled ? BackgroundJobCancellationService.MonitoringJobCanceledMessage : null
            });
        repo.Setup(r => r.HeartbeatMonitoringJobAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => heartbeatTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkMonitoringJobFailedAsync(1L, BackgroundJobCancellationService.MonitoringJobCanceledMessage, It.IsAny<CancellationToken>()))
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
        await Task.Delay(100);
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkMonitoringJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Never);
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