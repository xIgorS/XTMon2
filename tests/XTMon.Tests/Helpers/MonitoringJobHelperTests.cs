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

        var json = MonitoringJobHelper.SerializeTechnicalRejectColumns(columns, hasAlerts: true);
        var roundTrip = MonitoringJobHelper.DeserializeTechnicalRejectColumns(json);

        Assert.Equal(columns, roundTrip);
    }

    [Fact]
    public void SerializeTechnicalRejectColumns_ReturnsNullForEmptyWithNoAlerts()
    {
        var json = MonitoringJobHelper.SerializeTechnicalRejectColumns(Array.Empty<TechnicalRejectColumn>(), hasAlerts: false);

        Assert.Null(json);
    }

    [Fact]
    public void SerializeTechnicalRejectColumns_IncludesHasAlertsFlag()
    {
        var columns = new[] { new TechnicalRejectColumn("Col", "int") };

        var json = MonitoringJobHelper.SerializeTechnicalRejectColumns(columns, hasAlerts: true);

        Assert.NotNull(json);
        Assert.True(MonitoringJobHelper.TryGetHasAlertsFromMetadata(json, out var hasAlerts));
        Assert.True(hasAlerts);
    }

    [Fact]
    public void DeserializeTechnicalRejectColumns_HandlesLegacyArrayFormat()
    {
        var legacyJson = "[{\"Name\":\"TradeDate\",\"TypeName\":\"date\"}]";

        var result = MonitoringJobHelper.DeserializeTechnicalRejectColumns(legacyJson);

        Assert.Single(result);
        Assert.Equal("TradeDate", result[0].Name);
    }

    [Fact]
    public void TryGetHasAlertsFromMetadata_ReturnsFalseForLegacyArrayFormat()
    {
        var legacyJson = "[{\"Name\":\"TradeDate\",\"TypeName\":\"date\"}]";

        Assert.False(MonitoringJobHelper.TryGetHasAlertsFromMetadata(legacyJson, out _));
    }

    [Fact]
    public void TryGetHasAlertsFromMetadata_ReturnsFalseForMalformedJson()
    {
        Assert.False(MonitoringJobHelper.TryGetHasAlertsFromMetadata("not-json", out _));
        Assert.False(MonitoringJobHelper.TryGetHasAlertsFromMetadata(null, out _));
        Assert.False(MonitoringJobHelper.TryGetHasAlertsFromMetadata(string.Empty, out _));
    }

    [Fact]
    public void SerializeParameters_RoundTripsFunctionalRejectionParameters()
    {
        var parameters = new FunctionalRejectionJobParameters("CODE", 42, "FOO", "DTM");

        var json = MonitoringJobHelper.SerializeParameters(parameters);
        var roundTrip = MonitoringJobHelper.DeserializeParameters<FunctionalRejectionJobParameters>(json);

        Assert.Equal(parameters, roundTrip);
    }

    [Fact]
    public void ShouldTreatAsNotRun_ReturnsTrueForCancelledJobWithoutStartTime()
    {
        var result = MonitoringJobHelper.ShouldTreatAsNotRun(MonitoringJobHelper.CancelledStatus, startedAt: null);

        Assert.True(result);
    }

    [Fact]
    public void ShouldTreatAsNotRun_ReturnsFalseForCancelledJobAfterStart()
    {
        var result = MonitoringJobHelper.ShouldTreatAsNotRun(MonitoringJobHelper.CancelledStatus, DateTime.UtcNow);

        Assert.False(result);
    }
}