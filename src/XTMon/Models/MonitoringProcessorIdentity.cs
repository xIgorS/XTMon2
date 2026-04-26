namespace XTMon.Models;

public sealed record MonitoringProcessorIdentity(
    string Name,
    string Category,
    IReadOnlyList<string> IncludedSubmenuKeys,
    IReadOnlyList<string> ExcludedSubmenuKeys,
    int MaxConcurrentJobs);