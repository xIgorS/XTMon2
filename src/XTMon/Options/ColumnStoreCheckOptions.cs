using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class ColumnStoreCheckOptions
{
    public const string SectionName = "ColumnStoreCheck";

    [Required]
    public string ConnectionStringName { get; set; } = "LogFiAlmt";

    [Required]
    public string ColumnStoreCheckStoredProcedure { get; set; } = "monitoring.UspGetColStoreStatus";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}