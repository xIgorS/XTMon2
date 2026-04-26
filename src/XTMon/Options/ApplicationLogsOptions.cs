using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class ApplicationLogsOptions
{
    public const string SectionName = "ApplicationLogs";

    public bool Enabled { get; set; } = true;

    [Required]
    public string ConnectionStringName { get; set; } = "LogFiAlmt";

    [Required]
    public string GetApplicationLogsStoredProcedure { get; set; } = "monitoring.UspGetApplicationLogs";

    [Range(1, 300)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    [Range(1, 10000)]
    public int DefaultTopN { get; set; } = 200;

    [Range(1, 10000)]
    public int MaxTopN { get; set; } = 10000;

    [Range(1, 1440)]
    public int DefaultLookbackMinutes { get; set; } = 60;
}