using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MissingSogCheckOptions
{
    public const string SectionName = "MissingSogCheck";

    [Required]
    public string ConnectionStringName { get; set; } = "DtmFi";

    [Required]
    public string MissingSogCheckStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringMissingSOG";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}