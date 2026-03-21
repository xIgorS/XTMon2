using XTMon.Data;

namespace XTMon.Tests.Helpers;

public class JvCalculationHelperTests
{
    // ─── IsStaleRunningJob ───────────────────────────────────────────────────────

    [Fact]
    public void IsStaleRunningJob_WhenElapsedExceedsTimeout_ReturnsTrue()
    {
        var lastActivity = DateTime.UtcNow.AddHours(-2);
        var timeout = TimeSpan.FromMinutes(30);
        Assert.True(JvCalculationHelper.IsStaleRunningJob(lastActivity, timeout));
    }

    [Fact]
    public void IsStaleRunningJob_WhenElapsedEqualsTimeout_ReturnsTrue()
    {
        var timeout = TimeSpan.FromMinutes(30);
        var lastActivity = DateTime.UtcNow - timeout;
        Assert.True(JvCalculationHelper.IsStaleRunningJob(lastActivity, timeout));
    }

    [Fact]
    public void IsStaleRunningJob_WhenElapsedBelowTimeout_ReturnsFalse()
    {
        var lastActivity = DateTime.UtcNow.AddSeconds(-10);
        var timeout = TimeSpan.FromMinutes(30);
        Assert.False(JvCalculationHelper.IsStaleRunningJob(lastActivity, timeout));
    }

    // ─── ToUtc ──────────────────────────────────────────────────────────────────

    [Fact]
    public void ToUtc_WhenUtc_ReturnsSame()
    {
        var dt = new DateTime(2025, 3, 15, 10, 0, 0, DateTimeKind.Utc);
        var result = JvCalculationHelper.ToUtc(dt);
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(dt, result);
    }

    [Fact]
    public void ToUtc_WhenUnspecified_TreatsAsUtc()
    {
        var dt = new DateTime(2025, 3, 15, 10, 0, 0, DateTimeKind.Unspecified);
        var result = JvCalculationHelper.ToUtc(dt);
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(dt.Ticks, result.Ticks);
    }

    [Fact]
    public void ToUtc_WhenLocal_ConvertsToUtc()
    {
        var dt = new DateTime(2025, 3, 15, 10, 0, 0, DateTimeKind.Local);
        var result = JvCalculationHelper.ToUtc(dt);
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(dt.ToUniversalTime(), result);
    }

    // ─── ToHeaderLabel ───────────────────────────────────────────────────────────

    [Fact]
    public void ToHeaderLabel_WhenNull_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, JvCalculationHelper.ToHeaderLabel(null));
    }

    [Fact]
    public void ToHeaderLabel_WhenWhitespace_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, JvCalculationHelper.ToHeaderLabel("   "));
    }

    [Fact]
    public void ToHeaderLabel_SingleWord_ReturnsAsIs()
    {
        Assert.Equal("Amount", JvCalculationHelper.ToHeaderLabel("Amount"));
    }

    [Fact]
    public void ToHeaderLabel_CamelCase_InsertsSpaces()
    {
        Assert.Equal("Pnl Date", JvCalculationHelper.ToHeaderLabel("PnlDate"));
    }

    [Fact]
    public void ToHeaderLabel_MultiWordCamelCase_InsertsSpaces()
    {
        Assert.Equal("Grid Rows Json", JvCalculationHelper.ToHeaderLabel("GridRowsJson"));
    }

    [Fact]
    public void ToHeaderLabel_AcronymFollowedByWord_InsertsSpaceBeforeLower()
    {
        // "XMLParser" → "XMLParser" (no split in acronym, but space before "P" since prev=L upper, next=a lower)
        // Actually: X-M-L-P-a-r-s-e-r
        // At 'P': prev='L'(upper), next='a'(lower) → boundary=true → "XML Parser"
        Assert.Equal("XML Parser", JvCalculationHelper.ToHeaderLabel("XMLParser"));
    }

    // ─── GetColumnAlignmentClass ─────────────────────────────────────────────────

    [Theory]
    [InlineData("PnlDate")]
    [InlineData("StartTime")]
    [InlineData("ReplayStatus")]
    public void GetColumnAlignmentClass_TextKeywords_ReturnsTextClass(string columnName)
    {
        Assert.Equal("db-grid__cell--text", JvCalculationHelper.GetColumnAlignmentClass(columnName));
    }

    [Theory]
    [InlineData("FlowId")]
    [InlineData("TotalAmount")]
    [InlineData("CurrencyCode")]
    public void GetColumnAlignmentClass_NumKeywords_ReturnsNumClass(string columnName)
    {
        Assert.Equal("db-grid__cell--num", JvCalculationHelper.GetColumnAlignmentClass(columnName));
    }

    [Fact]
    public void GetColumnAlignmentClass_NoMatchingKeyword_DefaultsToTextClass()
    {
        Assert.Equal("db-grid__cell--text", JvCalculationHelper.GetColumnAlignmentClass("Description"));
    }

    // ─── DeserializeMonitoringTable ──────────────────────────────────────────────

    [Fact]
    public void DeserializeMonitoringTable_WhenBothNull_ReturnsNull()
    {
        Assert.Null(JvCalculationHelper.DeserializeMonitoringTable(null, null));
    }

    [Fact]
    public void DeserializeMonitoringTable_WhenColumnsBlank_ReturnsNull()
    {
        Assert.Null(JvCalculationHelper.DeserializeMonitoringTable("   ", "[[[\"a\"]]]"));
    }

    [Fact]
    public void DeserializeMonitoringTable_WhenRowsBlank_ReturnsNull()
    {
        Assert.Null(JvCalculationHelper.DeserializeMonitoringTable("[\"Col1\"]", "   "));
    }

    [Fact]
    public void DeserializeMonitoringTable_WhenInvalidJson_ReturnsNull()
    {
        Assert.Null(JvCalculationHelper.DeserializeMonitoringTable("not-json", "[[[\"a\"]]]"));
    }

    [Fact]
    public void DeserializeMonitoringTable_WhenValid_ReturnsTable()
    {
        var columnsJson = """["ColA","ColB"]""";
        var rowsJson = """[["val1","val2"],["val3",null]]""";

        var result = JvCalculationHelper.DeserializeMonitoringTable(columnsJson, rowsJson);

        Assert.NotNull(result);
        Assert.Equal(2, result.Columns.Count);
        Assert.Equal("ColA", result.Columns[0]);
        Assert.Equal("ColB", result.Columns[1]);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("val1", result.Rows[0][0]);
        Assert.Null(result.Rows[1][1]);
    }

    [Fact]
    public void DeserializeMonitoringTable_WhenEmptyArrays_ReturnsEmptyTable()
    {
        var result = JvCalculationHelper.DeserializeMonitoringTable("[]", "[]");
        Assert.NotNull(result);
        Assert.Empty(result.Columns);
        Assert.Empty(result.Rows);
    }
}
