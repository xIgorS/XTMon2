using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class FeedOutExtractionOptions
{
    public const string SectionName = "FeedOutExtraction";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string FeedOutExtractionStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringCheckFeedOutExtraction";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}