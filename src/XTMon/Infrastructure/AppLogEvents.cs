using Microsoft.Extensions.Logging;

namespace XTMon.Infrastructure;

public static class AppLogEvents
{
    // EventId ranges:
    // 1000s = Monitoring page/service
    // 2000s = Replay flows page/service
    // 3000s = Repository/data-access operations
    // 4000s = Background processing services
    public static readonly EventId MonitoringLoadFailed = new(1001, nameof(MonitoringLoadFailed));
    public static readonly EventId JvPageLoadFailed = new(1002, nameof(JvPageLoadFailed));
    public static readonly EventId JvPageActionFailed = new(1003, nameof(JvPageActionFailed));
    public static readonly EventId ReplayDataLoadFailed = new(2001, nameof(ReplayDataLoadFailed));
    public static readonly EventId ReplaySubmitFailed = new(2002, nameof(ReplaySubmitFailed));
    public static readonly EventId ReplayStatusLoadFailed = new(2003, nameof(ReplayStatusLoadFailed));
    public static readonly EventId ReplayPollingIntervalInvalid = new(2004, nameof(ReplayPollingIntervalInvalid));
    public static readonly EventId ReplayPollingLoopFailed = new(2005, nameof(ReplayPollingLoopFailed));

    public static readonly EventId RepositoryMonitoringProcedureFailed = new(3001, nameof(RepositoryMonitoringProcedureFailed));
    public static readonly EventId RepositoryFailedFlowsQueryFailed = new(3002, nameof(RepositoryFailedFlowsQueryFailed));
    public static readonly EventId RepositoryReplaySubmitEmptyRows = new(3003, nameof(RepositoryReplaySubmitEmptyRows));
    public static readonly EventId RepositoryReplaySubmitFailed = new(3004, nameof(RepositoryReplaySubmitFailed));
    public static readonly EventId RepositoryReplayProcessFailed = new(3005, nameof(RepositoryReplayProcessFailed));
    public static readonly EventId RepositoryReplayStatusFailed = new(3006, nameof(RepositoryReplayStatusFailed));
    public static readonly EventId RepositoryJvSqlTimeout = new(3007, nameof(RepositoryJvSqlTimeout));
    public static readonly EventId RepositoryJvConnectionFailed = new(3008, nameof(RepositoryJvConnectionFailed));
    public static readonly EventId RepositoryJvSlowOperation = new(3009, nameof(RepositoryJvSlowOperation));
    public static readonly EventId RepositoryMonitoringJobSqlTimeout = new(3010, nameof(RepositoryMonitoringJobSqlTimeout));
    public static readonly EventId RepositoryMonitoringJobConnectionFailed = new(3011, nameof(RepositoryMonitoringJobConnectionFailed));
    public static readonly EventId RepositoryMonitoringJobSlowOperation = new(3012, nameof(RepositoryMonitoringJobSlowOperation));
    public static readonly EventId RepositoryApplicationLogsQueryFailed = new(3013, nameof(RepositoryApplicationLogsQueryFailed));
    public static readonly EventId RepositoryApplicationLogsQuerySlow = new(3014, nameof(RepositoryApplicationLogsQuerySlow));
    public static readonly EventId RepositoryApplicationLogsQueryTimeout = new(3015, nameof(RepositoryApplicationLogsQueryTimeout));

    public static readonly EventId ReplayProcessorBackgroundFailed = new(4001, nameof(ReplayProcessorBackgroundFailed));
    public static readonly EventId JvProcessorSqlTimeout = new(4002, nameof(JvProcessorSqlTimeout));
    public static readonly EventId JvProcessorConnectionFailed = new(4003, nameof(JvProcessorConnectionFailed));
    public static readonly EventId JvProcessorBackgroundFailed = new(4004, nameof(JvProcessorBackgroundFailed));
    public static readonly EventId MonitoringProcessorSqlTimeout = new(4005, nameof(MonitoringProcessorSqlTimeout));
    public static readonly EventId MonitoringProcessorConnectionFailed = new(4006, nameof(MonitoringProcessorConnectionFailed));
    public static readonly EventId MonitoringProcessorBackgroundFailed = new(4007, nameof(MonitoringProcessorBackgroundFailed));

    public static readonly EventId DiagnosticsCheckFailed = new(5001, nameof(DiagnosticsCheckFailed));
}