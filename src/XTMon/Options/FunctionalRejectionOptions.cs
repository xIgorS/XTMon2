using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class FunctionalRejectionOptions
{
    public const string SectionName = "FunctionalRejection";

    [Required]
    public string MenuConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string StagingConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string DtmConnectionStringName { get; set; } = "DtmFi";

    [Required]
    public string SourceSystemTechnicalRejectStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringSourceSystemTechnicalReject";

    [Required]
    public string TechnicalRejectStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringTechnicalReject";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}