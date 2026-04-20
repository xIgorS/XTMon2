using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class PricingFileReceptionOptions
{
    public const string SectionName = "PricingFileReception";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string PricingFileReceptionStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringPricingReception";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}
