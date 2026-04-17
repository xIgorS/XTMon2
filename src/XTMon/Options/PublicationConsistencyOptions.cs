using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class PublicationConsistencyOptions
{
    public const string SectionName = "PublicationConsistency";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string PublicationConsistencyStoredProcedure { get; set; } = "monitoring.UspGetPublicationConsistency";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}