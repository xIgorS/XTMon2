using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class AdjustmentsOptions
{
    public const string SectionName = "Adjustments";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string AdjustmentsStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringAdjustments";

    [Required]
    public string GetAllSourceSystemsStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringGetAllSourceSystem";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}
