using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace XTMon.Services;

public sealed class ReplayFlowProcessingQueue
{
    private int _signalPending;

    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

    // This queue is a wake-up signal for the background processor, not a payload queue.
    // Coalescing repeated enqueues avoids silent drops and unnecessary backlog.
    // Safety: concurrent Enqueue calls are coalesced (only one signal is queued),
    // which is correct because the consumer (ReplayFlowProcessingService) calls
    // ProcessReplayFlowsAsync, which processes ALL pending items from the DB —
    // not just the work associated with the signal that triggered it.
    public async ValueTask EnqueueAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _signalPending, 1, 0) != 0)
        {
            return;
        }

        try
        {
            await _channel.Writer.WriteAsync(true, cancellationToken);
        }
        catch
        {
            Interlocked.Exchange(ref _signalPending, 0);
            throw;
        }
    }

    public async IAsyncEnumerable<bool> DequeueAllAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            Interlocked.Exchange(ref _signalPending, 0);
            yield return item;
        }
    }
}
