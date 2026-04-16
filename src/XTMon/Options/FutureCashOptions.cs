using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class FutureCashOptions
{
    public const string SectionName = "FutureCash";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string FutureCashStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringCheckFutureCash";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}