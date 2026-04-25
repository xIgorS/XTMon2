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

    [Fact]
    public void GetSafeBackgroundJobMessage_WhenStoredErrorHasInternalDetails_ReturnsFallback()
    {
        var result = MonitoringDisplayHelper.GetSafeBackgroundJobMessage(
            "SqlException: Login failed for server PROD_SQL. Procedure dbo.Secret failed.",
            "Unable to load data right now. Please try again.");

        Assert.Equal("Unable to load data right now. Please try again.", result);
    }

    [Fact]
    public void GetSafeBackgroundJobMessage_WhenFallbackBlank_ReturnsGenericMessage()
    {
        var result = MonitoringDisplayHelper.GetSafeBackgroundJobMessage("internal detail", " ");

        Assert.Equal("The background job failed.", result);
    }

    [Fact]
    public void GetMonitoringJobCompletionTime_WhenCompletedAtPresent_ReturnsFormattedTime()
    {
        var completedAt = new DateTime(2025, 1, 1, 10, 7, 30);
        var job = new MonitoringJobRecord(
            1,
            "DataValidation",
            "batch-status",
            null,
            new DateOnly(2025, 1, 1),
            "Completed",
            null,
            null,
            null,
            new DateTime(2025, 1, 1, 10, 0, 0),
            new DateTime(2025, 1, 1, 10, 5, 0),
            null,
            completedAt,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

        var expected = DateTime.SpecifyKind(completedAt, DateTimeKind.Utc)
            .ToLocalTime()
            .ToString("dd-MM-yyyy HH:mm:ss");

        Assert.Equal(expected, MonitoringDisplayHelper.GetMonitoringJobCompletionTime(job));
    }

    [Fact]
    public void GetMonitoringJobCompletionTime_WhenNoTerminalTimestamp_ReturnsDash()
    {
        var job = new MonitoringJobRecord(
            1,
            "DataValidation",
            "batch-status",
            null,
            new DateOnly(2025, 1, 1),
            "Running",
            null,
            null,
            null,
            new DateTime(2025, 1, 1, 10, 0, 0),
            new DateTime(2025, 1, 1, 10, 5, 0),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

        Assert.Equal("-", MonitoringDisplayHelper.GetMonitoringJobCompletionTime(job));
    }

    [Fact]
    public void GetMonitoringJobDuration_WhenCompletedJob_ReturnsElapsedDuration()
    {
        var job = new MonitoringJobRecord(
            1,
            "DataValidation",
            "batch-status",
            null,
            new DateOnly(2025, 1, 1),
            "Completed",
            null,
            null,
            null,
            new DateTime(2025, 1, 1, 10, 0, 0),
            new DateTime(2025, 1, 1, 10, 5, 0),
            null,
            new DateTime(2025, 1, 1, 10, 7, 30),
            null,
            null,
            null,
            null,
            null,
            null,
            null);

        Assert.Equal("2m 30s", MonitoringDisplayHelper.GetMonitoringJobDuration(job));
    }

    [Fact]
    public void GetMonitoringJobDuration_WhenRunningJob_UsesProvidedNowUtc()
    {
        var job = new MonitoringJobRecord(
            1,
            "DataValidation",
            "batch-status",
            null,
            new DateOnly(2025, 1, 1),
            "Running",
            null,
            null,
            null,
            new DateTime(2025, 1, 1, 10, 0, 0),
            new DateTime(2025, 1, 1, 10, 5, 0),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

        Assert.Equal(
            "1m 05s",
            MonitoringDisplayHelper.GetMonitoringJobDuration(job, new DateTime(2025, 1, 1, 10, 6, 5, DateTimeKind.Utc)));
    }

    [Fact]
    public void GetMonitoringJobDuration_WhenNotStarted_ReturnsDash()
    {
        var job = new MonitoringJobRecord(
            1,
            "DataValidation",
            "batch-status",
            null,
            new DateOnly(2025, 1, 1),
            "Queued",
            null,
            null,
            null,
            new DateTime(2025, 1, 1, 10, 0, 0),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

        Assert.Equal("-", MonitoringDisplayHelper.GetMonitoringJobDuration(job));
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
