using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class SystemDiagnosticsOptions
{
    public const string SectionName = "SystemDiagnostics";

    [Required]
    public string ConnectionStringName { get; set; } = "LogFiAlmt";

    public bool ShowCleanupButtons { get; set; }

    [Required]
    public string CleanLoggingStoredProcedure { get; set; } = "monitoring.UspSystemDiagnosticsCleanLogging";

    [Required]
    public string CleanHistoryStoredProcedure { get; set; } = "monitoring.UspSystemDiagnosticsCleanHistory";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 120;
}