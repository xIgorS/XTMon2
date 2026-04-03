using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class BatchStatusOptions
{
    public const string SectionName = "BatchStatus";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string CheckBatchStatusStoredProcedure { get; set; } = "administration.UspCheckBatchStatus";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}