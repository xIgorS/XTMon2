using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class RejectedXtgPortfolioOptions
{
    public const string SectionName = "RejectedXtgPortfolio";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string RejectedXtgPortfolioStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringPortfolioXTGRejected";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}