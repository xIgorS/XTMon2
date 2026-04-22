using System.Collections.Concurrent;

namespace XTMon.Services;

public sealed class JobCancellationRegistry
{
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _monitoringJobTokens = new();
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _jvJobTokens = new();

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

        cancellationTokenSource.Cancel();
        return true;
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

        cancellationTokenSource.Cancel();
        return true;
    }
}