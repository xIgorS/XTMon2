using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MultipleFeedVersionOptions
{
    public const string SectionName = "MultipleFeedVersion";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string MultipleFeedVersionStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringMultipleFeedVersion";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}