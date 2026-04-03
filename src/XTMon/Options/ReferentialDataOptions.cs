using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class ReferentialDataOptions
{
    public const string SectionName = "ReferentialData";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string CheckReferentialDataStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringCheckReferentiel";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}