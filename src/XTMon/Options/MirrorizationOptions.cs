using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MirrorizationOptions
{
    public const string SectionName = "Mirrorization";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string MirrorizationStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringCarrySpread";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}