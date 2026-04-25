using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class FunctionalRejectionSelectionHelperTests
{
    private static readonly FunctionalRejectionMenuItem MenuItem = new("ABC_CA", 12, "FOO", "STAGING");

    [Fact]
    public void ContainsSelection_ReturnsTrue_WhenSelectionMatchesMenuItemIgnoringCase()
    {
        var result = FunctionalRejectionSelectionHelper.ContainsSelection(
            [MenuItem],
            "abc_ca",
            12,
            "foo",
            "staging");

        Assert.True(result);
    }

    [Fact]
    public void ContainsSelection_ReturnsFalse_WhenCodeDoesNotMatch()
    {
        var result = FunctionalRejectionSelectionHelper.ContainsSelection(
            [MenuItem],
            "OTHER",
            12,
            "FOO",
            "STAGING");

        Assert.False(result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(-1)]
    public void ContainsSelection_ReturnsFalse_WhenBusinessDataTypeIdIsInvalid(int? businessDataTypeId)
    {
        var result = FunctionalRejectionSelectionHelper.ContainsSelection(
            [MenuItem],
            "ABC_CA",
            businessDataTypeId,
            "FOO",
            "STAGING");

        Assert.False(result);
    }
}