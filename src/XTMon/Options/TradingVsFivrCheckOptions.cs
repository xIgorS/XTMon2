using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class TradingVsFivrCheckOptions
{
    public const string SectionName = "TradingVsFivrCheck";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string TradingVsFivrCheckStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringTradingVsFivr";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}