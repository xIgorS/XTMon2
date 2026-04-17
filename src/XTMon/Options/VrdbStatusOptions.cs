using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class VrdbStatusOptions
{
    public const string SectionName = "VrdbStatus";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string VrdbStatusStoredProcedure { get; set; } = "monitoring.UspGetVRDBPricingReceptionStatus";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}