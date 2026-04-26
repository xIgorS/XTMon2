using XTMon.Helpers;

namespace XTMon.Tests.Helpers;

public class ApplicationLogFilterHelperTests
{
    [Fact]
    public void NormalizeLevels_TrimsDeduplicatesAndCanonicalizesValidLevels()
    {
        var result = ApplicationLogFilterHelper.NormalizeLevels(new[] { " warning ", "ERROR", "Warning", "fatal", "Foo", null, " " });

        Assert.Equal(new[] { "Warning", "Error", "Fatal" }, result);
    }

    [Fact]
    public void NormalizeLevels_WhenNull_ReturnsEmpty()
    {
        var result = ApplicationLogFilterHelper.NormalizeLevels(null);

        Assert.Empty(result);
    }

    [Theory]
    [InlineData(0, 200, 10000, 200)]
    [InlineData(-5, 200, 10000, 200)]
    [InlineData(500, 200, 10000, 500)]
    [InlineData(12000, 200, 10000, 10000)]
    public void ClampTopN_ClampsAsExpected(int requested, int defaultN, int maxN, int expected)
    {
        var result = ApplicationLogFilterHelper.ClampTopN(requested, defaultN, maxN);

        Assert.Equal(expected, result);
    }

    [Fact]
    public void ResolveTimeRange_WhenBothNull_UsesDefaultLookback()
    {
        var nowUtc = new DateTime(2026, 4, 26, 12, 0, 0, DateTimeKind.Utc);

        var result = ApplicationLogFilterHelper.ResolveTimeRange(null, null, 60, nowUtc);

        Assert.Equal(nowUtc.AddMinutes(-60), result.From);
        Assert.Equal(nowUtc, result.To);
    }

    [Fact]
    public void ResolveTimeRange_WhenReversed_SwapsValues()
    {
        var from = new DateTime(2026, 4, 26, 13, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 4, 26, 12, 0, 0, DateTimeKind.Utc);

        var result = ApplicationLogFilterHelper.ResolveTimeRange(from, to, 60, DateTime.UtcNow);

        Assert.Equal(to, result.From);
        Assert.Equal(from, result.To);
    }

    [Fact]
    public void ResolveTimeRange_WhenOnlyFromProvided_UsesNowForTo()
    {
        var nowUtc = new DateTime(2026, 4, 26, 12, 0, 0, DateTimeKind.Utc);
        var from = new DateTime(2026, 4, 26, 11, 30, 0, DateTimeKind.Utc);

        var result = ApplicationLogFilterHelper.ResolveTimeRange(from, null, 60, nowUtc);

        Assert.Equal(from, result.From);
        Assert.Equal(nowUtc, result.To);
    }

    [Fact]
    public void ToCsv_WhenEmpty_ReturnsNull()
    {
        var result = ApplicationLogFilterHelper.ToCsv(Array.Empty<string>());

        Assert.Null(result);
    }

    [Fact]
    public void ToCsv_WhenValuesPresent_JoinsValues()
    {
        var result = ApplicationLogFilterHelper.ToCsv(new[] { "Warning", "Error", "Fatal" });

        Assert.Equal("Warning,Error,Fatal", result);
    }
}