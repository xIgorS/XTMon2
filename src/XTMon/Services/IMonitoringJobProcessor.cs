using XTMon.Models;

namespace XTMon.Services;

public interface IMonitoringJobProcessor
{
    MonitoringProcessorIdentity Identity { get; }
}