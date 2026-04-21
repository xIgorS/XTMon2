using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class DataValidationBatchRunHelperTests
{
    [Fact]
    public void BuildDefaultParametersJson_ReturnsSourceSystemDefaultsForPricing()
    {
        var json = DataValidationBatchRunHelper.BuildDefaultParametersJson("pricing", defaultJvPrecision: 0.25m);
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(json);

        Assert.NotNull(parameters);
        Assert.Null(parameters.SourceSystemCodes);
        Assert.Null(parameters.TraceAllVersions);
        Assert.Null(parameters.Precision);
        Assert.Equal("All source systems", DataValidationBatchRunHelper.BuildDefaultParameterSummary("pricing", 0.25m));
    }

    [Fact]
    public void BuildDefaultParametersJson_ReturnsTraceAllVersionsFalseForPricingFileReception()
    {
        var json = DataValidationBatchRunHelper.BuildDefaultParametersJson("pricing-file-reception", defaultJvPrecision: 0.25m);
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(json);

        Assert.NotNull(parameters);
        Assert.False(parameters.TraceAllVersions);
        Assert.Equal("Trace all versions: No", DataValidationBatchRunHelper.BuildDefaultParameterSummary("pricing-file-reception", 0.25m));
    }

    [Fact]
    public void BuildDefaultParametersJson_ReturnsConfiguredPrecisionForJvBalance()
    {
        var json = DataValidationBatchRunHelper.BuildDefaultParametersJson("jv-balance-consistency", defaultJvPrecision: 1.235m);
        var parameters = MonitoringJobHelper.DeserializeParameters<DataValidationJobParameters>(json);

        Assert.NotNull(parameters);
        Assert.Equal(1.235m, parameters.Precision);
        Assert.Equal("Precision: 1.24", DataValidationBatchRunHelper.BuildDefaultParameterSummary("jv-balance-consistency", 1.235m));
    }

    [Fact]
    public void BuildDefaultParametersJson_ReturnsNullForChecksWithoutExtraParameters()
    {
        Assert.Null(DataValidationBatchRunHelper.BuildDefaultParametersJson("market-data", defaultJvPrecision: 0.25m));
        Assert.Null(DataValidationBatchRunHelper.BuildDefaultParameterSummary("market-data", 0.25m));
    }
}