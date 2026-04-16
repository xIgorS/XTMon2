using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class OutOfScopePortfolioOptions
{
    public const string SectionName = "OutOfScopePortfolio";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string OutOfScopePortfolioStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringOutOfScope";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}