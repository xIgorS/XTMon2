using XTMon.Helpers;

namespace XTMon.Tests.Helpers;

public class DataValidationCheckCatalogTests
{
    [Fact]
    public void Find_ReturnsRestrictedBatchStatusDefinition()
    {
        var definition = DataValidationCheckCatalog.Find("batch-status");

        Assert.NotNull(definition);
        Assert.Equal("Batch Status", definition.DisplayName);
        Assert.True(definition.RequiresRestrictedAccess);
    }

    [Fact]
    public void IsKnownRoute_ReturnsTrueForKnownCheck()
    {
        var result = DataValidationCheckCatalog.IsKnownRoute("jv-balance-consistency");

        Assert.True(result);
    }

    [Fact]
    public void Routes_MatchDeclaredChecks()
    {
        Assert.Equal(DataValidationCheckCatalog.Checks.Count, DataValidationCheckCatalog.Routes.Count);
        Assert.Contains("pricing", DataValidationCheckCatalog.Routes);
        Assert.Contains("vrdb-status", DataValidationCheckCatalog.Routes);
    }
}