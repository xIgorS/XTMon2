using XTMon.Services;

namespace XTMon.Tests.Services;

public class JobCancellationRegistryTests
{
    [Fact]
    public void CancelMonitoringJob_ReturnsFalse_WhenRegisteredSourceWasDisposed()
    {
        var registry = new JobCancellationRegistry();
        var cancellationTokenSource = new CancellationTokenSource();
        registry.RegisterMonitoringJob(1L, cancellationTokenSource);
        cancellationTokenSource.Dispose();

        var cancelled = registry.CancelMonitoringJob(1L);

        Assert.False(cancelled);
    }

    [Fact]
    public void CancelAllMonitoringJobs_SkipsDisposedSources_AndCancelsActiveOnes()
    {
        var registry = new JobCancellationRegistry();
        var disposedSource = new CancellationTokenSource();
        var activeSource = new CancellationTokenSource();
        registry.RegisterMonitoringJob(1L, disposedSource);
        registry.RegisterMonitoringJob(2L, activeSource);
        disposedSource.Dispose();

        var cancelledCount = registry.CancelAllMonitoringJobs();

        Assert.Equal(1, cancelledCount);
        Assert.True(activeSource.IsCancellationRequested);
    }

    [Fact]
    public async Task CancelAllMonitoringJobs_WakesCurrentWaiters_WithoutLeavingStaleSignals()
    {
        var registry = new JobCancellationRegistry();
        using var firstSource = new CancellationTokenSource();
        using var secondSource = new CancellationTokenSource();
        registry.RegisterMonitoringJob(1L, firstSource);
        registry.RegisterMonitoringJob(2L, secondSource);

        var firstWait = registry.WaitForMonitoringJobCancellationAsync(CancellationToken.None);
        var secondWait = registry.WaitForMonitoringJobCancellationAsync(CancellationToken.None);

        var cancelledCount = registry.CancelAllMonitoringJobs();

        Assert.Equal(2, cancelledCount);
        await Task.WhenAll(
            firstWait.WaitAsync(TimeSpan.FromSeconds(1)),
            secondWait.WaitAsync(TimeSpan.FromSeconds(1)));

        using var thirdSource = new CancellationTokenSource();
        registry.RegisterMonitoringJob(3L, thirdSource);

        var nextWait = registry.WaitForMonitoringJobCancellationAsync(CancellationToken.None);

        await Assert.ThrowsAsync<TimeoutException>(() => nextWait.WaitAsync(TimeSpan.FromMilliseconds(150)));

        Assert.True(registry.CancelMonitoringJob(3L));

        await nextWait.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void CancelJvJob_ReturnsFalse_WhenRegisteredSourceWasDisposed()
    {
        var registry = new JobCancellationRegistry();
        var cancellationTokenSource = new CancellationTokenSource();
        registry.RegisterJvJob(1L, cancellationTokenSource);
        cancellationTokenSource.Dispose();

        var cancelled = registry.CancelJvJob(1L);

        Assert.False(cancelled);
    }

    [Fact]
    public async Task CancelAllJvJobs_WakesCurrentWaiters_WithoutLeavingStaleSignals()
    {
        var registry = new JobCancellationRegistry();
        using var firstSource = new CancellationTokenSource();
        using var secondSource = new CancellationTokenSource();
        registry.RegisterJvJob(1L, firstSource);
        registry.RegisterJvJob(2L, secondSource);

        var firstWait = registry.WaitForJvJobCancellationAsync(CancellationToken.None);
        var secondWait = registry.WaitForJvJobCancellationAsync(CancellationToken.None);

        var cancelledCount = registry.CancelAllJvJobs();

        Assert.Equal(2, cancelledCount);
        await Task.WhenAll(
            firstWait.WaitAsync(TimeSpan.FromSeconds(1)),
            secondWait.WaitAsync(TimeSpan.FromSeconds(1)));

        using var thirdSource = new CancellationTokenSource();
        registry.RegisterJvJob(3L, thirdSource);

        var nextWait = registry.WaitForJvJobCancellationAsync(CancellationToken.None);

        await Assert.ThrowsAsync<TimeoutException>(() => nextWait.WaitAsync(TimeSpan.FromMilliseconds(150)));

        Assert.True(registry.CancelJvJob(3L));

        await nextWait.WaitAsync(TimeSpan.FromSeconds(1));
    }
}