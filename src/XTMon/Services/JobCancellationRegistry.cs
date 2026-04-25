using System.Collections.Concurrent;

namespace XTMon.Services;

public sealed class JobCancellationRegistry
{
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _monitoringJobTokens = new();
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _jvJobTokens = new();
    private readonly object _monitoringJobCancellationSignalGate = new();
    private readonly object _jvJobCancellationSignalGate = new();
    private TaskCompletionSource<bool> _monitoringJobCancellationSignal = CreateCancellationSignal();
    private TaskCompletionSource<bool> _jvJobCancellationSignal = CreateCancellationSignal();

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

        SignalMonitoringJobCancellationRequested();
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
            SignalMonitoringJobCancellationRequested();
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
        Task signalTask;
        lock (_monitoringJobCancellationSignalGate)
        {
            signalTask = _monitoringJobCancellationSignal.Task;
        }

        return signalTask.WaitAsync(cancellationToken);
    }

    public void SignalJvJobCancellationRequested()
    {
        TaskCompletionSource<bool> signalToRelease;
        lock (_jvJobCancellationSignalGate)
        {
            signalToRelease = _jvJobCancellationSignal;
            _jvJobCancellationSignal = CreateCancellationSignal();
        }

        signalToRelease.TrySetResult(true);
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

        if (!TryCancel(cancellationTokenSource))
        {
            return false;
        }

        SignalJvJobCancellationRequested();
        return true;
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

        if (cancelledCount > 0)
        {
            SignalJvJobCancellationRequested();
        }

        return cancelledCount;
    }

    public Task WaitForJvJobCancellationAsync(CancellationToken cancellationToken)
    {
        Task signalTask;
        lock (_jvJobCancellationSignalGate)
        {
            signalTask = _jvJobCancellationSignal.Task;
        }

        return signalTask.WaitAsync(cancellationToken);
    }

    private void SignalMonitoringJobCancellationRequested()
    {
        TaskCompletionSource<bool> signalToRelease;
        lock (_monitoringJobCancellationSignalGate)
        {
            signalToRelease = _monitoringJobCancellationSignal;
            _monitoringJobCancellationSignal = CreateCancellationSignal();
        }

        signalToRelease.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> CreateCancellationSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
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