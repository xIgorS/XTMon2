using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class BatchStatusHelperTests
{
    [Fact]
    public void BuildGridRows_WhenTableIsNull_ReturnsEmpty()
    {
        var result = BatchStatusHelper.BuildGridRows(null);

        Assert.Empty(result);
    }

    [Fact]
    public void BuildGridRows_WhenRequiredColumnMissing_Throws()
    {
        var table = new MonitoringTableResult(
            ["pnldate", "ConsoIsDone", "DatetimeEndCalculation", "DatetimeEndExtraction"],
            new List<IReadOnlyList<string?>>());

        var exception = Assert.Throws<InvalidOperationException>(() => BatchStatusHelper.BuildGridRows(table));

        Assert.Contains("CalculationIsDone", exception.Message);
    }

    [Fact]
    public void BuildGridRows_WhenBothFlagsAreOne_SetsStatusToOkAndFormatsDateTimeParts()
    {
        var table = new MonitoringTableResult(
            ["pnldate", "ConsoIsDone", "CalculationIsDone", "DatetimeEndCalculation", "DatetimeEndExtraction"],
            [new List<string?> { "2026-03-31", "1", "1", "2026-04-01 13:45:30", "2026-04-01 14:10:05" }]);

        var result = BatchStatusHelper.BuildGridRows(table);

        var row = Assert.Single(result);
        Assert.Equal("OK", row.Status);
        Assert.Equal("31-03-2026", row.PnlDate);
        Assert.Equal("01-04-2026", row.CalculationDate);
        Assert.Equal("13:45:30", row.CalculationEndTime);
        Assert.Equal("14:10:05", row.ExtractionEndTime);
    }

    [Fact]
    public void BuildGridRows_WhenEitherFlagIsNotOne_SetsStatusToKo()
    {
        var table = new MonitoringTableResult(
            ["pnldate", "ConsoIsDone", "CalculationIsDone", "DatetimeEndCalculation", "DatetimeEndExtraction"],
            [new List<string?> { "2026-03-31", "1", "0", "2026-04-01 13:45:30", "2026-04-01 14:10:05" }]);

        var result = BatchStatusHelper.BuildGridRows(table);

        Assert.Equal("KO", Assert.Single(result).Status);
    }

    [Fact]
    public void BuildGridRows_WhenDateTimesAreNull_UsesDashPlaceholders()
    {
        var table = new MonitoringTableResult(
            ["pnldate", "ConsoIsDone", "CalculationIsDone", "DatetimeEndCalculation", "DatetimeEndExtraction"],
            [new List<string?> { "2026-03-31", "0", "0", null, null }]);

        var result = BatchStatusHelper.BuildGridRows(table);

        var row = Assert.Single(result);
        Assert.Equal("KO", row.Status);
        Assert.Equal("31-03-2026", row.PnlDate);
        Assert.Equal("-", row.CalculationDate);
        Assert.Equal("-", row.CalculationEndTime);
        Assert.Equal("-", row.ExtractionEndTime);
    }
}