using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class RolloveredPortfoliosOptions
{
    public const string SectionName = "RolloveredPortfolios";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string RolloveredPortfoliosStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringPTFRolled";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}