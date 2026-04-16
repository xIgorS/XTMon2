using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class NonXtgPortfolioOptions
{
    public const string SectionName = "NonXtgPortfolio";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string NonXtgPortfolioStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringPortfolioFlaggedXTG";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}