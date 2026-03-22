using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MonitoringOptions
{
    public const string SectionName = "Monitoring";

    [Required]
    public string ConnectionStringName { get; set; } = "LogFiAlmt";

    [Required]
    public string DbSizePlusDiskStoredProcedure { get; set; } = "monitoring.UspGetDbSizePlusDisk";

    [Required]
    public string DbBackupsStoredProcedure { get; set; } = "monitoring.UspGetDBBackups";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}
