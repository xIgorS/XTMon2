using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class ResultTransferOptions
{
    public const string SectionName = "ResultTransfer";

    [Required]
    public string ConnectionStringName { get; set; } = "StagingFiAlmt";

    [Required]
    public string ResultTransferStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringResultTransfer";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}