namespace XTMon.Models;

public sealed record MonitoringProcessorHealthReport(
    IReadOnlyList<MonitoringProcessorHealthRow> Rows,
    DateTime GeneratedAtUtc,
    TimeSpan QueueBacklogGracePeriod,
    bool DmvAvailable)
{
    public int IssueCount => Rows.Count(row => row.HasIssue || row.IsRuntimeCheckUnavailable);

    public bool HasIssues => IssueCount > 0;
}

public sealed record MonitoringProcessorHealthRow(
    string ProcessorKey,
    string DisplayName,
    string Category,
    IReadOnlyList<string> IncludedSubmenuKeys,
    IReadOnlyList<string> ExcludedSubmenuKeys,
    int ConfiguredWorkers,
    int QueuedJobs,
    int RunningJobs,
    int LiveRuntimeJobs,
    DateTime? OldestQueuedAtUtc,
    bool HasIssue,
    bool IsRuntimeCheckUnavailable,
    string Status,
    string Detail);