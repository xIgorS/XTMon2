using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MissingWorkflowCheckOptions
{
    public const string SectionName = "MissingWorkflowCheck";

    [Required]
    public string ConnectionStringName { get; set; } = "Publication";

    [Required]
    public string MissingWorkflowCheckStoredProcedure { get; set; } = "monitoring.UspCheckIfPortfolioCreatedInWorkflow";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}