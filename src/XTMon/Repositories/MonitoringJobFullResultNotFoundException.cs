namespace XTMon.Repositories;

internal sealed class MonitoringJobFullResultNotFoundException : Exception
{
    public MonitoringJobFullResultNotFoundException(long monitoringJobId)
        : base($"No persisted full CSV result exists for monitoring job {monitoringJobId}.")
    {
        MonitoringJobId = monitoringJobId;
    }

    public long MonitoringJobId { get; }
}