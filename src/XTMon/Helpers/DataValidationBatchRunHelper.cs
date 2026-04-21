using System.Globalization;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class DataValidationBatchRunHelper
{
    public static string? BuildDefaultParametersJson(string route, decimal defaultJvPrecision)
    {
        return MonitoringJobHelper.BuildDataValidationSubmenuKey(route) switch
        {
            "daily-balance" or "adjustments" or "pricing" or "reverse-conso-file"
                => MonitoringJobHelper.SerializeParameters(new DataValidationJobParameters()),
            "pricing-file-reception"
                => MonitoringJobHelper.SerializeParameters(new DataValidationJobParameters(TraceAllVersions: false)),
            "jv-balance-consistency"
                => MonitoringJobHelper.SerializeParameters(new DataValidationJobParameters(Precision: defaultJvPrecision)),
            _ => null
        };
    }

    public static string? BuildDefaultParameterSummary(string route, decimal defaultJvPrecision)
    {
        return MonitoringJobHelper.BuildDataValidationSubmenuKey(route) switch
        {
            "daily-balance" or "adjustments" or "pricing" or "reverse-conso-file" => "All source systems",
            "pricing-file-reception" => "Trace all versions: No",
            "jv-balance-consistency" => string.Create(
                CultureInfo.InvariantCulture,
                $"Precision: {FormatPrecision(defaultJvPrecision)}"),
            _ => null
        };
    }

    public static string FormatPrecision(decimal precision)
    {
        return Math.Round(precision, 2, MidpointRounding.AwayFromZero).ToString("0.00", CultureInfo.InvariantCulture);
    }
}