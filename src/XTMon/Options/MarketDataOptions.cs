using System.ComponentModel.DataAnnotations;

namespace XTMon.Options;

public sealed class MarketDataOptions
{
    public const string SectionName = "MarketData";

    [Required]
    public string ConnectionStringName { get; set; } = "DtmFi";

    [Required]
    public string MarketDataStoredProcedure { get; set; } = "monitoring.UspXtgMonitoringMarketData";

    [Range(1, 3600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}