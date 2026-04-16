using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class SasTablesOptions
{
    public const string SectionName = "SasTables";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string SasTablesStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringSAS";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}