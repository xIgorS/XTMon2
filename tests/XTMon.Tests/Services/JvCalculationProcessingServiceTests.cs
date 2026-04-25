using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Reflection;
using XTMon.Helpers;
using XTMon.Repositories;
using XTMon.Services;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Tests.Services;

public class JvCalculationProcessingServiceTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    // ─── helpers ────────────────────────────────────────────────────────────────

    private static IOptions<JvCalculationOptions> DefaultOptions() =>
        Microsoft.Extensions.Options.Options.Create(new JvCalculationOptions
        {
            JobRunningStaleTimeoutSeconds = 1800,
            // All other required properties left at defaults (empty strings);
            // they are only used by the concrete repository which isn't called here.
        });

    private static JvJobRecord MakeJob(string requestType, long jobId = 1L) =>
        new JvJobRecord(
            JobId: jobId,
            UserId: "user1",
            PnlDate: TestDate,
            RequestType: requestType,
            Status: "Queued",
            WorkerId: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: null,
            LastHeartbeatAt: null,
            CompletedAt: null,
            FailedAt: null,
            ErrorMessage: null,
            QueryCheck: null,
            QueryFix: null,
            GridColumnsJson: null,
            GridRowsJson: null,
            SavedAt: null);

    private static (JvCalculationProcessingService service, Mock<IJvCalculationRepository> repo)
        CreateService(
            Mock<IJvCalculationRepository> repo,
            IOptions<JvCalculationOptions>? options = null,
            TimeSpan? idleDelay = null,
            TimeSpan? heartbeatInterval = null,
            JobCancellationRegistry? jobCancellationRegistry = null)
    {
        var sp = new Mock<IServiceProvider>();
        sp.Setup(p => p.GetService(typeof(IJvCalculationRepository))).Returns(repo.Object);

        var scope = new Mock<IServiceScope>();
        scope.Setup(s => s.ServiceProvider).Returns(sp.Object);
        scope.Setup(s => s.Dispose());

        var factory = new Mock<IServiceScopeFactory>();
        factory.Setup(f => f.CreateScope()).Returns(scope.Object);

        var service = new JvCalculationProcessingService(
            factory.Object,
            options ?? DefaultOptions(),
            NullLogger<JvCalculationProcessingService>.Instance,
            idleDelay ?? TimeSpan.FromSeconds(5),
            heartbeatInterval,
            jobCancellationRegistry);

        return (service, repo);
    }

    private static Mock<IJvCalculationRepository> BaseRepo()
    {
        var repo = new Mock<IJvCalculationRepository>();
        repo.Setup(r => r.ExpireStaleRunningJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);
        repo.Setup(r => r.HeartbeatJvJobAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.SaveJvJobResultAsync(It.IsAny<long>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<MonitoringTableResult?>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkJvJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.MarkJvJobFailedAsync(It.IsAny<long>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.GetJvJobByIdAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((long jobId, CancellationToken _) => MakeJob("CheckOnly", jobId) with
            {
                Status = "Running",
                StartedAt = DateTime.UtcNow
            });
        return repo;
    }

    // ─── tests ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task WhenNoJobAvailable_DoesNotProcessAnyJob()
    {
        var repo = BaseRepo();
        repo.Setup(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((JvJobRecord?)null);

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        await EventuallyAsync(() => repo.Invocations.Any(invocation => invocation.Method.Name == nameof(IJvCalculationRepository.TryTakeNextJvJobAsync)));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkJvJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
        repo.Verify(r => r.MarkJvJobFailedAsync(It.IsAny<long>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckOnlyJob_DoesNotCallFix()
    {
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new JvCalculationCheckResult("SELECT 1", new XTMon.Models.MonitoringTableResult([], [])));
        repo.Setup(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.FixJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        repo.Verify(r => r.CheckJvCalculationAsync(TestDate, It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FixAndCheckJob_CallsFixThenCheck()
    {
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("FixAndCheck");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.FixJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("SELECT fix");
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new JvCalculationCheckResult("SELECT check", new XTMon.Models.MonitoringTableResult([], [])));
        repo.Setup(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.FixJvCalculationAsync(TestDate, true, It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.CheckJvCalculationAsync(TestDate, It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task WhenJobThrows_CallsMarkJvJobFailedAsync()
    {
        var failedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("check failed"));
        repo.Setup(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => failedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        await failedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkJvJobCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task OnEachPoll_CallsExpireStaleRunningJobs()
    {
        var twoExpiresTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = BaseRepo();
        repo.Setup(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.ExpireStaleRunningJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                if (repo.Invocations.Count(invocation => invocation.Method.Name == nameof(IJvCalculationRepository.ExpireStaleRunningJobsAsync)) >= 2)
                {
                    twoExpiresTcs.TrySetResult(true);
                }
                return 0;
            });

        var (svc, _) = CreateService(repo, idleDelay: TimeSpan.FromMilliseconds(10));
        await svc.StartAsync(CancellationToken.None);

        await twoExpiresTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(
            r => r.ExpireStaleRunningJobsAsync(It.IsAny<TimeSpan>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(2));
    }

    [Fact]
    public async Task WhenMarkFailedThrowsOnFirstAttempt_RetriesAndSucceeds()
    {
        var failedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("check failed"));
        repo.SetupSequence(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("db unavailable"))
            .Returns(() =>
            {
                failedTcs.TrySetResult(true);
                return Task.CompletedTask;
            });

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        await failedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task WhenMarkFailedThrowsOnAllAttempts_DoesNotRetryMoreThanMax()
    {
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("check failed"));
        repo.Setup(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("db unavailable"));

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        // Allow enough time for all retry attempts plus 2s retry delays in between.
        await Task.Delay(TimeSpan.FromSeconds(8));
        await svc.StopAsync(CancellationToken.None);

        // MarkStateMaxAttempts = 3; should never exceed that, even if all throw.
        repo.Verify(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Fact]
    public async Task HeartbeatIsCalledBeforeProcessing()
    {
        var callOrder = new List<string>();
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.HeartbeatJvJobAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("heartbeat"))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("check"))
            .ReturnsAsync(new JvCalculationCheckResult("SELECT 1", new XTMon.Models.MonitoringTableResult([], [])));
        repo.Setup(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var (svc, _) = CreateService(repo);
        await svc.StartAsync(CancellationToken.None);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await svc.StopAsync(CancellationToken.None);

        Assert.Equal("heartbeat", callOrder[0]);
        Assert.Contains("check", callOrder);
    }

    [Fact]
    public async Task LongRunningJob_SendsPeriodicHeartbeatsWhileExecuting()
    {
        var heartbeatCount = 0;
        var repeatedHeartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCheckTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.HeartbeatJvJobAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() =>
            {
                heartbeatCount++;
                if (heartbeatCount >= 3)
                {
                    repeatedHeartbeatTcs.TrySetResult(true);
                }
            })
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .Returns((DateOnly _, CancellationToken token) => Task.Run(async () =>
            {
                await releaseCheckTcs.Task.WaitAsync(TimeSpan.FromSeconds(5), token);
                return new JvCalculationCheckResult("SELECT 1", new MonitoringTableResult([], []));
            }, token));
        repo.Setup(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => completedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var (svc, _) = CreateService(
            repo,
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(20));

        await svc.StartAsync(CancellationToken.None);

        await repeatedHeartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        releaseCheckTcs.TrySetResult(true);

        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.HeartbeatJvJobAsync(1L, It.IsAny<CancellationToken>()), Times.AtLeast(3));
    }

    [Fact]
    public async Task LongRunningJob_WhenHeartbeatTimesOut_StopsWorkerAndMarksFailed()
    {
        var executorCancelledTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var failedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.SetupSequence(r => r.HeartbeatJvJobAsync(1L, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .ThrowsAsync(MakeSqlException(-2, "Heartbeat timeout"));
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .Returns((DateOnly _, CancellationToken token) => Task.Run(async () =>
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

                return new JvCalculationCheckResult("SELECT 1", new MonitoringTableResult([], []));
            }, token));
        repo.Setup(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(() => failedTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        var (svc, _) = CreateService(
            repo,
            idleDelay: TimeSpan.FromMilliseconds(10),
            heartbeatInterval: TimeSpan.FromMilliseconds(20));

        await svc.StartAsync(CancellationToken.None);

        await executorCancelledTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await failedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await svc.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkJvJobFailedAsync(1L, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        repo.Verify(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task WhenJvJobIsCancelled_DoesNotMarkCompleted()
    {
        var heartbeatTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationRequestedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var job = MakeJob("CheckOnly");
        var registry = new JobCancellationRegistry();
        var isCancelled = false;

        var repo = BaseRepo();
        repo.SetupSequence(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(job)
            .ReturnsAsync((JvJobRecord?)null);
        repo.Setup(r => r.GetJvJobByIdAsync(1L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => MakeJob("CheckOnly") with
            {
                Status = isCancelled ? MonitoringJobHelper.CancelledStatus : "Running",
                FailedAt = isCancelled ? DateTime.UtcNow : null,
                ErrorMessage = isCancelled ? BackgroundJobCancellationService.JvJobCanceledMessage : null
            });
        repo.Setup(r => r.HeartbeatJvJobAsync(1L, It.IsAny<CancellationToken>()))
            .Callback(() => heartbeatTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);
        repo.Setup(r => r.CheckJvCalculationAsync(It.IsAny<DateOnly>(), It.IsAny<CancellationToken>()))
            .Returns(async (DateOnly _, CancellationToken token) =>
            {
                await heartbeatTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
                isCancelled = true;
                registry.CancelJvJob(1L);
                cancellationRequestedTcs.TrySetResult(true);
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new JvCalculationCheckResult("SELECT 1", new MonitoringTableResult([], []));
            });
        repo.Setup(r => r.MarkJvJobCancelledAsync(1L, BackgroundJobCancellationService.JvJobCanceledMessage, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var (service, _) = CreateService(repo, idleDelay: TimeSpan.FromMilliseconds(10), jobCancellationRegistry: registry);

        await service.StartAsync(CancellationToken.None);

        await cancellationRequestedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        repo.Verify(r => r.MarkJvJobCompletedAsync(1L, It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CancellationSignal_WakesIdlePollLoop()
    {
        var firstPollTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPollTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = new JobCancellationRegistry();
        var pollCount = 0;

        var repo = BaseRepo();
        repo.Setup(r => r.TryTakeNextJvJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                var currentPoll = Interlocked.Increment(ref pollCount);
                if (currentPoll == 1)
                {
                    firstPollTcs.TrySetResult(true);
                }
                else if (currentPoll == 2)
                {
                    secondPollTcs.TrySetResult(true);
                }

                return (JvJobRecord?)null;
            });

        var (service, _) = CreateService(repo, idleDelay: TimeSpan.FromSeconds(30), jobCancellationRegistry: registry);
        await service.StartAsync(CancellationToken.None);

        try
        {
            await firstPollTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            registry.SignalJvJobCancellationRequested();
            await secondPollTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }

        Assert.True(Volatile.Read(ref pollCount) >= 2);
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
}
