using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class JvBalanceConsistencyOptions
{
    public const string SectionName = "JvBalanceConsistency";

    [Required]
    public string ConnectionStringName { get; set; } = "Publication";

    [Required]
    public string JvBalanceConsistencyStoredProcedure { get; set; } = "monitoring.UspCheckJvBalanceBetweenTwoDates";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;

    [Range(0d, 99.99d)]
    public decimal Precision { get; set; } = 0.01m;
}