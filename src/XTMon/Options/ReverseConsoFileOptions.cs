using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class ReverseConsoFileOptions
{
    public const string SectionName = "ReverseConsoFile";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string ReverseConsoFileStoredProcedure { get; set; } = "monitoring.UspXtgMTRevConWorkflowCheck";

    [Required]
    public string GetAllSourceSystemsStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringGetAllrevConSourceSystem";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}