using System.Collections.Concurrent;

namespace XTMon.Services;

public sealed class JobCancellationRegistry
{
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _monitoringJobTokens = new();
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _jvJobTokens = new();
    private readonly SemaphoreSlim _monitoringJobCancellationSignal = new(0);
    private readonly SemaphoreSlim _jvJobCancellationSignal = new(0);

    public void RegisterMonitoringJob(long jobId, CancellationTokenSource cancellationTokenSource)
    {
        _monitoringJobTokens.AddOrUpdate(jobId, cancellationTokenSource, (_, _) => cancellationTokenSource);
    }

    public void UnregisterMonitoringJob(long jobId, CancellationTokenSource cancellationTokenSource)
    {
        if (_monitoringJobTokens.TryGetValue(jobId, out var current) && ReferenceEquals(current, cancellationTokenSource))
        {
            _monitoringJobTokens.TryRemove(jobId, out _);
        }
    }

    public bool CancelMonitoringJob(long jobId)
    {
        if (!_monitoringJobTokens.TryGetValue(jobId, out var cancellationTokenSource))
        {
            return false;
        }

        if (!TryCancel(cancellationTokenSource))
        {
            return false;
        }

        _monitoringJobCancellationSignal.Release();
        return true;
    }

    public int CancelAllMonitoringJobs()
    {
        var cancelledCount = 0;

        foreach (var entry in _monitoringJobTokens)
        {
            if (!TryCancel(entry.Value))
            {
                continue;
            }

            cancelledCount++;
        }

        if (cancelledCount > 0)
        {
            _monitoringJobCancellationSignal.Release(cancelledCount);
        }

        return cancelledCount;
    }

    public bool IsMonitoringJobCancellationRequested(long jobId)
    {
        return _monitoringJobTokens.TryGetValue(jobId, out var cancellationTokenSource)
            && cancellationTokenSource.IsCancellationRequested;
    }

    public Task WaitForMonitoringJobCancellationAsync(CancellationToken cancellationToken)
    {
        return _monitoringJobCancellationSignal.WaitAsync(cancellationToken);
    }

    public void SignalJvJobCancellationRequested()
    {
        _jvJobCancellationSignal.Release();
    }

    public void RegisterJvJob(long jobId, CancellationTokenSource cancellationTokenSource)
    {
        _jvJobTokens.AddOrUpdate(jobId, cancellationTokenSource, (_, _) => cancellationTokenSource);
    }

    public void UnregisterJvJob(long jobId, CancellationTokenSource cancellationTokenSource)
    {
        if (_jvJobTokens.TryGetValue(jobId, out var current) && ReferenceEquals(current, cancellationTokenSource))
        {
            _jvJobTokens.TryRemove(jobId, out _);
        }
    }

    public bool CancelJvJob(long jobId)
    {
        if (!_jvJobTokens.TryGetValue(jobId, out var cancellationTokenSource))
        {
            return false;
        }

        return TryCancel(cancellationTokenSource);
    }

    public int CancelAllJvJobs()
    {
        var cancelledCount = 0;

        foreach (var entry in _jvJobTokens)
        {
            if (!TryCancel(entry.Value))
            {
                continue;
            }

            cancelledCount++;
        }

        return cancelledCount;
    }

    public Task WaitForJvJobCancellationAsync(CancellationToken cancellationToken)
    {
        return _jvJobCancellationSignal.WaitAsync(cancellationToken);
    }

    private static bool TryCancel(CancellationTokenSource cancellationTokenSource)
    {
        if (cancellationTokenSource.IsCancellationRequested)
        {
            return false;
        }

        try
        {
            cancellationTokenSource.Cancel();
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }
}