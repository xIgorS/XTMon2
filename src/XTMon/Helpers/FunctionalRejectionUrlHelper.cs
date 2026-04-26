using System.Globalization;
using Microsoft.AspNetCore.WebUtilities;
using XTMon.Models;

namespace XTMon.Helpers;

internal static class FunctionalRejectionUrlHelper
{
    public const string Route = "functional-rejection";

    public static string BuildHref(FunctionalRejectionMenuItem item, string? pnlDate = null)
    {
        ArgumentNullException.ThrowIfNull(item);

        return BuildHref(
            item.SourceSystemBusinessDataTypeCode,
            item.BusinessDataTypeId,
            item.SourceSystemName,
            item.DbConnection,
            pnlDate);
    }

    public static string BuildHref(
        string? code,
        int? businessDataTypeId,
        string? sourceSystemName,
        string? dbConnection,
        string? pnlDate = null)
    {
        return QueryHelpers.AddQueryString(
            Route,
            new Dictionary<string, string?>
            {
                ["code"] = code,
                ["businessDatatypeId"] = businessDataTypeId?.ToString(CultureInfo.InvariantCulture),
                ["sourceSystemName"] = sourceSystemName,
                ["dbConnection"] = dbConnection,
                ["pnlDate"] = pnlDate
            });
    }
}