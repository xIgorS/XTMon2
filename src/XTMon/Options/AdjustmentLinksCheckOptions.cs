using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class AdjustmentLinksCheckOptions
{
    public const string SectionName = "AdjustmentLinksCheck";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string AdjustmentLinksCheckStoredProcedure { get; set; } = "monitoring.UspGetAdjNotlink";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}