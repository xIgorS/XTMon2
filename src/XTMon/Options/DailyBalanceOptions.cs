using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class DailyBalanceOptions
{
    public const string SectionName = "DailyBalance";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string DailyBalanceStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringBalancesCalculation";

    [Required]
    public string GetAllSourceSystemsStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringGetAllSourceSystem";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}
