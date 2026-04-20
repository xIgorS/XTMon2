using XTMon.Helpers;

namespace XTMon.Tests.Helpers;

public class PricingHelperTests
{
    [Fact]
    public void BuildSourceSystemCodes_WhenEmpty_ReturnsNull()
    {
        var result = PricingHelper.BuildSourceSystemCodes(Array.Empty<string>());

        Assert.Null(result);
    }

    [Fact]
    public void BuildSourceSystemCodes_TrimsAndJoinsCodes()
    {
        var result = PricingHelper.BuildSourceSystemCodes([" XT1 ", "XT2", "XT3 "]);

        Assert.Equal("XT1,XT2,XT3", result);
    }

    [Fact]
    public void BuildSourceSystemCodes_RemovesDuplicatesIgnoringCase()
    {
        var result = PricingHelper.BuildSourceSystemCodes(["XT1", "xt1", "XT2", "XT2", "XT3"]);

        Assert.Equal("XT1,XT2,XT3", result);
    }

    [Fact]
    public void BuildSourceSystemCodes_IgnoresBlankEntries()
    {
        var result = PricingHelper.BuildSourceSystemCodes(["XT1", " ", null, "XT2"]);

        Assert.Equal("XT1,XT2", result);
    }

    [Fact]
    public void BuildSourceSystemCodes_WhenQuoteEachValue_QuotesEachCode()
    {
        var result = PricingHelper.BuildSourceSystemCodes([" PAR1 ", "PaR2"], quoteEachValue: true);

        Assert.Equal("'PAR1','PaR2'", result);
    }

    [Fact]
    public void BuildSourceSystemCodes_WhenQuoteEachValue_EscapesEmbeddedQuotes()
    {
        var result = PricingHelper.BuildSourceSystemCodes(["PA'R1"], quoteEachValue: true);

        Assert.Equal("'PA''R1'", result);
    }
}