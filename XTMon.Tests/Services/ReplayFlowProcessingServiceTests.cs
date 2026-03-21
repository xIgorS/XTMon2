using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XTMon.Data;

namespace XTMon.Tests.Services;

public class ReplayFlowProcessingServiceTests
{
    // ─── helpers ────────────────────────────────────────────────────────────────

    private static (ReplayFlowProcessingService service, Mock<IReplayFlowRepository> repo)
        CreateService(Action<Mock<IReplayFlowRepository>>? configure = null)
    {
        var repo = new Mock<IReplayFlowRepository>();
        configure?.Invoke(repo);

        var scopeFactory = BuildScopeFactory(repo.Object);
        var queue = new ReplayFlowProcessingQueue();
        var logger = NullLogger<ReplayFlowProcessingService>.Instance;

        var service = new ReplayFlowProcessingService(scopeFactory, queue, logger);
        return (service, repo);
    }

    private static IServiceScopeFactory BuildScopeFactory(IReplayFlowRepository repository)
    {
        var sp = new Mock<IServiceProvider>();
        sp.Setup(p => p.GetService(typeof(IReplayFlowRepository))).Returns(repository);

        var scope = new Mock<IServiceScope>();
        scope.Setup(s => s.ServiceProvider).Returns(sp.Object);
        scope.Setup(s => s.Dispose());

        var factory = new Mock<IServiceScopeFactory>();
        factory.Setup(f => f.CreateScope()).Returns(scope.Object);

        return factory.Object;
    }

    // ─── tests ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StartAndStop_WithNoItems_StopsCleanly()
    {
        var (service, repo) = CreateService();

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        repo.Verify(x => x.ProcessReplayFlowsAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task WhenItemDequeued_CallsProcessReplayFlowsAsync()
    {
        var processedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // We need to access the queue that was injected; rebuild so we control the queue
        var repo2 = new Mock<IReplayFlowRepository>();
        repo2.Setup(x => x.ProcessReplayFlowsAsync(It.IsAny<CancellationToken>()))
             .Callback(() => processedTcs.TrySetResult(true))
             .Returns(Task.CompletedTask);

        var scopeFactory = BuildScopeFactory(repo2.Object);
        var queue = new ReplayFlowProcessingQueue();
        var logger = NullLogger<ReplayFlowProcessingService>.Instance;
        var svc = new ReplayFlowProcessingService(scopeFactory, queue, logger);

        await svc.StartAsync(CancellationToken.None);
        await queue.EnqueueAsync(CancellationToken.None);

        var completed = await processedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(completed);

        await svc.StopAsync(CancellationToken.None);
        repo2.Verify(x => x.ProcessReplayFlowsAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task WhenProcessReplayFlowsThrows_ServiceContinues()
    {
        // First call throws, second call succeeds — service should not crash
        var callCount = 0;
        var secondCallTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var repo = new Mock<IReplayFlowRepository>();
        repo.Setup(x => x.ProcessReplayFlowsAsync(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount == 1)
                {
                    throw new InvalidOperationException("Simulated transient failure");
                }
                secondCallTcs.TrySetResult(true);
                return Task.CompletedTask;
            });

        var scopeFactory = BuildScopeFactory(repo.Object);
        var queue = new ReplayFlowProcessingQueue();
        var logger = NullLogger<ReplayFlowProcessingService>.Instance;
        var svc = new ReplayFlowProcessingService(scopeFactory, queue, logger);

        await svc.StartAsync(CancellationToken.None);

        await queue.EnqueueAsync(CancellationToken.None);
        await queue.EnqueueAsync(CancellationToken.None);

        var secondSucceeded = await secondCallTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(secondSucceeded);

        await svc.StopAsync(CancellationToken.None);
        Assert.Equal(2, callCount);
    }
}
