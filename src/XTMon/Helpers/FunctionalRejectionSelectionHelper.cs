using XTMon.Models;

namespace XTMon.Helpers;

internal static class FunctionalRejectionSelectionHelper
{
    public static bool MatchesSelection(
        FunctionalRejectionMenuItem item,
        string? sourceSystemBusinessDataTypeCode,
        int? businessDataTypeId,
        string? sourceSystemName,
        string? dbConnection)
    {
        return businessDataTypeId.HasValue &&
            businessDataTypeId.Value > 0 &&
            string.Equals(item.SourceSystemBusinessDataTypeCode, sourceSystemBusinessDataTypeCode, StringComparison.OrdinalIgnoreCase) &&
            item.BusinessDataTypeId == businessDataTypeId.Value &&
            string.Equals(item.SourceSystemName, sourceSystemName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(item.DbConnection, dbConnection, StringComparison.OrdinalIgnoreCase);
    }

    public static bool ContainsSelection(
        IEnumerable<FunctionalRejectionMenuItem> items,
        string? sourceSystemBusinessDataTypeCode,
        int? businessDataTypeId,
        string? sourceSystemName,
        string? dbConnection)
    {
        return items.Any(item => MatchesSelection(
            item,
            sourceSystemBusinessDataTypeCode,
            businessDataTypeId,
            sourceSystemName,
            dbConnection));
    }
}