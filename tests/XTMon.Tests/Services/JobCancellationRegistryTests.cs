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
    public async Task CancelAllMonitoringJobs_ReleasesSignalForEachCancelledSource()
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
}