using XTMon.Helpers;
using XTMon.Models;

namespace XTMon.Tests.Helpers;

public class MonitoringJobHelperTests
{
    [Fact]
    public void BuildDataValidationSubmenuKey_NormalizesRoute()
    {
        var result = MonitoringJobHelper.BuildDataValidationSubmenuKey("/Batch-Status ");

        Assert.Equal("batch-status", result);
    }

    [Fact]
    public void BuildFunctionalRejectionSubmenuKey_IsDeterministic()
    {
        var left = MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(14, "SYS A", "STAGING", "ABC");
        var right = MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(14, "SYS A", "STAGING", "ABC");

        Assert.Equal(left, right);
    }

    [Fact]
    public void SerializeTechnicalRejectColumns_RoundTrips()
    {
        var columns = new[]
        {
            new TechnicalRejectColumn("TradeDate", "date"),
            new TechnicalRejectColumn("Amount", "decimal")
        };

        var json = MonitoringJobHelper.SerializeTechnicalRejectColumns(columns);
        var roundTrip = MonitoringJobHelper.DeserializeTechnicalRejectColumns(json);

        Assert.Equal(columns, roundTrip);
    }

    [Fact]
    public void SerializeParameters_RoundTripsFunctionalRejectionParameters()
    {
        var parameters = new FunctionalRejectionJobParameters("CODE", 42, "FOO", "DTM");

        var json = MonitoringJobHelper.SerializeParameters(parameters);
        var roundTrip = MonitoringJobHelper.DeserializeParameters<FunctionalRejectionJobParameters>(json);

        Assert.Equal(parameters, roundTrip);
    }
}