using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class PricingOptions
{
    public const string SectionName = "Pricing";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string PricingStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringPricingDaily";

    [Required]
    public string GetAllSourceSystemsStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringGetAllSourceSystem";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}