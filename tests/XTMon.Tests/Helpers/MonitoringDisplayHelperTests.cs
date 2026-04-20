using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class MonitoringDisplayHelperTests
{
    // ─── NormalizeColumnName ──────────────────────────────────────────────────────

    [Fact]
    public void NormalizeColumnName_WhenNull_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, MonitoringDisplayHelper.NormalizeColumnName(null));
    }

    [Fact]
    public void NormalizeColumnName_WhenWhitespace_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, MonitoringDisplayHelper.NormalizeColumnName("   "));
    }

    [Fact]
    public void NormalizeColumnName_RemovesUnderscoresAndSpaces()
    {
        Assert.Equal("lastupdateddate", MonitoringDisplayHelper.NormalizeColumnName("Last_Updated Date"));
    }

    [Fact]
    public void NormalizeColumnName_ConvertsToLowerCase()
    {
        Assert.Equal("alertlevel", MonitoringDisplayHelper.NormalizeColumnName("AlertLevel"));
    }

    // ─── FormatDateTimeForDisplay ─────────────────────────────────────────────────

    [Fact]
    public void FormatDateTimeForDisplay_WhenValidDateTime_FormatsCorrectly()
    {
        var result = MonitoringDisplayHelper.FormatDateTimeForDisplay("2025-03-15 14:30:45");
        Assert.Equal("15-03-2025 14:30:45", result);
    }

    [Fact]
    public void FormatDateTimeForDisplay_WhenInvalidDateTime_ReturnsOriginal()
    {
        var result = MonitoringDisplayHelper.FormatDateTimeForDisplay("not-a-date");
        Assert.Equal("not-a-date", result);
    }

    [Fact]
    public void FormatDateTimeForDisplay_WhenDateOnly_FormatsWithZeroTime()
    {
        var result = MonitoringDisplayHelper.FormatDateTimeForDisplay("2025-01-01");
        Assert.Equal("01-01-2025 00:00:00", result);
    }

    // ─── IsDateLikeColumn ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData("PnlDate", true)]
    [InlineData("DateTime", true)]
    [InlineData("LastUpdated", true)]
    [InlineData("CreatedTime", true)]
    [InlineData("AlertLevel", false)]
    [InlineData("DatabaseName", false)]
    [InlineData("SpaceAllocatedMB", false)]
    public void IsDateLikeColumn_ReturnsExpected(string columnName, bool expected)
    {
        Assert.Equal(expected, MonitoringDisplayHelper.IsDateLikeColumn(columnName));
    }

    [Fact]
    public void IsDateLikeColumn_HandlesUnderscoresAndSpaces()
    {
        Assert.True(MonitoringDisplayHelper.IsDateLikeColumn("Last_Updated_Date"));
    }

    // ─── FormatWithSpaces (long) ──────────────────────────────────────────────────

    [Fact]
    public void FormatWithSpaces_Long_SmallNumber_NoSpaces()
    {
        Assert.Equal("42", MonitoringDisplayHelper.FormatWithSpaces(42L));
    }

    [Fact]
    public void FormatWithSpaces_Long_ThousandsSeparator()
    {
        Assert.Equal("1 234 567", MonitoringDisplayHelper.FormatWithSpaces(1_234_567L));
    }

    [Fact]
    public void FormatWithSpaces_Long_Zero()
    {
        Assert.Equal("0", MonitoringDisplayHelper.FormatWithSpaces(0L));
    }

    [Fact]
    public void FormatWithSpaces_Long_Negative()
    {
        Assert.Equal("-1 000", MonitoringDisplayHelper.FormatWithSpaces(-1000L));
    }

    // ─── FormatWithSpaces (decimal) ───────────────────────────────────────────────

    [Fact]
    public void FormatWithSpaces_Decimal_WholeNumber_StripsTrailingZeros()
    {
        Assert.Equal("1 000", MonitoringDisplayHelper.FormatWithSpaces(1000m));
    }

    [Fact]
    public void FormatWithSpaces_Decimal_WithOneDecimal_StripsTrailingZero()
    {
        Assert.Equal("1 234.5", MonitoringDisplayHelper.FormatWithSpaces(1234.50m));
    }

    [Fact]
    public void FormatWithSpaces_Decimal_WithTwoDecimals_KeepsBoth()
    {
        Assert.Equal("99.99", MonitoringDisplayHelper.FormatWithSpaces(99.99m));
    }

    [Fact]
    public void FormatWithSpaces_Decimal_Zero()
    {
        Assert.Equal("0", MonitoringDisplayHelper.FormatWithSpaces(0m));
    }

    // ─── FormatCurrencyWithSpaces ──────────────────────────────────────────────

    [Fact]
    public void FormatCurrencyWithSpaces_WholeNumber_KeepsTwoDecimals()
    {
        Assert.Equal("1 000.00", MonitoringDisplayHelper.FormatCurrencyWithSpaces(1000m));
    }

    [Fact]
    public void FormatCurrencyWithSpaces_OneDecimal_KeepsTrailingZero()
    {
        Assert.Equal("1 234.50", MonitoringDisplayHelper.FormatCurrencyWithSpaces(1234.5m));
    }

    [Fact]
    public void FormatCurrencyWithSpaces_TwoDecimals_KeepsTwoDecimals()
    {
        Assert.Equal("99.99", MonitoringDisplayHelper.FormatCurrencyWithSpaces(99.99m));
    }

    // ─── FindLastUpdatedColumnIndex ───────────────────────────────────────────────

    [Fact]
    public void FindLastUpdatedColumnIndex_WhenPresent_ReturnsIndex()
    {
        var columns = new List<string> { "DatabaseName", "LastUpdated", "AlertLevel" };
        Assert.Equal(1, MonitoringDisplayHelper.FindLastUpdatedColumnIndex(columns));
    }

    [Fact]
    public void FindLastUpdatedColumnIndex_WhenLastUpdatedDate_ReturnsIndex()
    {
        var columns = new List<string> { "Name", "LastUpdatedDate" };
        Assert.Equal(1, MonitoringDisplayHelper.FindLastUpdatedColumnIndex(columns));
    }

    [Fact]
    public void FindLastUpdatedColumnIndex_WhenNotPresent_ReturnsNegativeOne()
    {
        var columns = new List<string> { "DatabaseName", "AlertLevel" };
        Assert.Equal(-1, MonitoringDisplayHelper.FindLastUpdatedColumnIndex(columns));
    }

    [Fact]
    public void FindLastUpdatedColumnIndex_CaseInsensitive()
    {
        var columns = new List<string> { "LASTUPDATED" };
        Assert.Equal(0, MonitoringDisplayHelper.FindLastUpdatedColumnIndex(columns));
    }

    [Fact]
    public void FindLastUpdatedColumnIndex_HandlesUnderscores()
    {
        var columns = new List<string> { "Last_Updated" };
        Assert.Equal(0, MonitoringDisplayHelper.FindLastUpdatedColumnIndex(columns));
    }

    // ─── GetLastUpdatedDisplayValue ───────────────────────────────────────────────

    [Fact]
    public void GetLastUpdatedDisplayValue_WhenNull_ReturnsNull()
    {
        Assert.Null(MonitoringDisplayHelper.GetLastUpdatedDisplayValue(null));
    }

    [Fact]
    public void GetLastUpdatedDisplayValue_WhenNoRows_ReturnsNull()
    {
        var result = new MonitoringTableResult(
            new List<string> { "LastUpdated" },
            new List<IReadOnlyList<string?>>());
        Assert.Null(MonitoringDisplayHelper.GetLastUpdatedDisplayValue(result));
    }

    [Fact]
    public void GetLastUpdatedDisplayValue_WhenNoLastUpdatedColumn_ReturnsNull()
    {
        var result = new MonitoringTableResult(
            new List<string> { "DatabaseName" },
            new List<IReadOnlyList<string?>> { new List<string?> { "TestDB" } });
        Assert.Null(MonitoringDisplayHelper.GetLastUpdatedDisplayValue(result));
    }

    [Fact]
    public void GetLastUpdatedDisplayValue_WhenValid_ReturnsFormatted()
    {
        var result = new MonitoringTableResult(
            new List<string> { "DatabaseName", "LastUpdated" },
            new List<IReadOnlyList<string?>> { new List<string?> { "TestDB", "2025-06-15 10:30:00" } });
        Assert.Equal("15-06-2025 10:30:00", MonitoringDisplayHelper.GetLastUpdatedDisplayValue(result));
    }

    [Fact]
    public void GetLastUpdatedDisplayValue_SkipsNullValues_ReturnsFirstNonNull()
    {
        var result = new MonitoringTableResult(
            new List<string> { "LastUpdated" },
            new List<IReadOnlyList<string?>>
            {
                new List<string?> { null },
                new List<string?> { "2025-01-01 08:00:00" }
            });
        Assert.Equal("01-01-2025 08:00:00", MonitoringDisplayHelper.GetLastUpdatedDisplayValue(result));
    }

    [Fact]
    public void GetLastUpdatedDisplayValue_WhenRowTooShort_SkipsIt()
    {
        var result = new MonitoringTableResult(
            new List<string> { "Name", "LastUpdated" },
            new List<IReadOnlyList<string?>>
            {
                new List<string?> { "OnlyOneColumn" },
                new List<string?> { "DB", "2025-12-25 12:00:00" }
            });
        Assert.Equal("25-12-2025 12:00:00", MonitoringDisplayHelper.GetLastUpdatedDisplayValue(result));
    }
}
