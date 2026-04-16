using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class PricingFileReceptionOptions
{
    public const string SectionName = "PricingFileReception";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string PricingFileReceptionStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringPricingReception";

    [Required]
    public string GetAllSourceSystemsStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringGetAllSourceSystem";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}
