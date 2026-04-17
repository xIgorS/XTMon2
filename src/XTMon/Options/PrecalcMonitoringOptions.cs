using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class PrecalcMonitoringOptions
{
    public const string SectionName = "PrecalcMonitoring";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string PrecalcMonitoringStoredProcedure { get; set; } = "core_process.UspCheckIfTableCreatedAfterPreCalc";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}