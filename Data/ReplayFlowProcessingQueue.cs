using System.Threading.Channels;

namespace XTMon.Data;

public sealed class ReplayFlowProcessingQueue
{
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(10)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        });

    public ValueTask EnqueueAsync(CancellationToken cancellationToken = default) =>
        _channel.Writer.WriteAsync(true, cancellationToken);

    public IAsyncEnumerable<bool> DequeueAllAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);
}
