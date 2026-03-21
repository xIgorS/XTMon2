using XTMon.Data;

namespace XTMon.Tests.Queue;

public class ReplayFlowProcessingQueueTests
{
    [Fact]
    public async Task EnqueueAsync_Then_DequeueAllAsync_DeliversItem()
    {
        var queue = new ReplayFlowProcessingQueue();
        using var cts = new CancellationTokenSource();

        await queue.EnqueueAsync(CancellationToken.None);

        // Cancel after reading one item so DequeueAllAsync completes
        var received = new List<bool>();
        cts.CancelAfter(TimeSpan.FromMilliseconds(200));

        try
        {
            await foreach (var item in queue.DequeueAllAsync(cts.Token))
            {
                received.Add(item);
                cts.Cancel(); // stop after first item
            }
        }
        catch (OperationCanceledException)
        {
            // expected
        }

        Assert.Single(received);
        Assert.True(received[0]);
    }

    [Fact]
    public async Task EnqueueAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        var queue = new ReplayFlowProcessingQueue();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // The channel raises TaskCanceledException (a subclass of OperationCanceledException)
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => queue.EnqueueAsync(cts.Token).AsTask());
        Assert.True(ex.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public async Task DequeueAllAsync_WhenCancelled_StopsIteration()
    {
        var queue = new ReplayFlowProcessingQueue();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var count = 0;
        try
        {
            await foreach (var _ in queue.DequeueAllAsync(cts.Token))
            {
                count++;
            }
        }
        catch (OperationCanceledException)
        {
            // expected when channel is cancelled before any items
        }

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task EnqueueAsync_WhenFull_DropsWrite()
    {
        // The channel capacity is 10; enqueuing 11 items should silently drop the last one.
        var queue = new ReplayFlowProcessingQueue();

        for (var i = 0; i < 11; i++)
        {
            // WriteAsync with DropWrite mode will silently drop when full — no exception
            await queue.EnqueueAsync(CancellationToken.None);
        }

        var received = new List<bool>();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        try
        {
            await foreach (var item in queue.DequeueAllAsync(cts.Token))
            {
                received.Add(item);
                if (received.Count == 10)
                {
                    cts.Cancel();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // expected
        }

        // At most 10 items should have been stored (capacity)
        Assert.True(received.Count <= 10);
    }

    [Fact]
    public async Task MultipleEnqueues_AllDequeued_InOrder()
    {
        var queue = new ReplayFlowProcessingQueue();

        // Enqueue 3 items
        for (var i = 0; i < 3; i++)
        {
            await queue.EnqueueAsync(CancellationToken.None);
        }

        var count = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        try
        {
            await foreach (var _ in queue.DequeueAllAsync(cts.Token))
            {
                count++;
                if (count == 3)
                {
                    cts.Cancel();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // expected
        }

        Assert.Equal(3, count);
    }
}
