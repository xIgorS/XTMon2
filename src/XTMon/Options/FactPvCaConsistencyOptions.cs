using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class FactPvCaConsistencyOptions
{
    public const string SectionName = "FactPvCaConsistency";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string FactPvCaConsistencyStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringCheckConsistencyEvent";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}