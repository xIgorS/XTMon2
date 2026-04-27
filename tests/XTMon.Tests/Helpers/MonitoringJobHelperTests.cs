using System.IO.Compression;
using System.Text;
using System.Text.Json;
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
    public void TruncateRows_DoesNotTruncate_WhenMaxRowsIsZero()
    {
        var source = new MonitoringTableResult(
            ["Col1"],
            [["A"], ["B"], ["C"]]);

        var result = MonitoringJobHelper.TruncateRows(source, maxRows: 0, out var totalRowCount, out var truncated);

        Assert.Same(source, result);
        Assert.Equal(3, totalRowCount);
        Assert.False(truncated);
    }

    [Fact]
    public void TruncateRows_DoesNotTruncate_WhenMaxRowsExceedsRowCount()
    {
        var source = new MonitoringTableResult(
            ["Col1"],
            [["A"], ["B"]]);

        var result = MonitoringJobHelper.TruncateRows(source, maxRows: 5, out var totalRowCount, out var truncated);

        Assert.Same(source, result);
        Assert.Equal(2, totalRowCount);
        Assert.False(truncated);
    }

    [Fact]
    public void TruncateRows_Truncates_WhenMaxRowsIsSmallerThanRowCount()
    {
        var source = new MonitoringTableResult(
            ["Col1"],
            [["A"], ["B"], ["C"]]);

        var result = MonitoringJobHelper.TruncateRows(source, maxRows: 2, out var totalRowCount, out var truncated);

        Assert.NotSame(source, result);
        Assert.Equal(3, totalRowCount);
        Assert.True(truncated);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("A", result.Rows[0][0]);
        Assert.Equal("B", result.Rows[1][0]);
    }

    [Fact]
    public void BuildPersistMetadataJson_RoundTripsCounts_AndPreservesNestedExtra()
    {
        var extra = MonitoringJobHelper.SerializeTechnicalRejectColumns(
            [new TechnicalRejectColumn("TradeDate", "date")],
            hasAlerts: true);

        var metadataJson = MonitoringJobHelper.BuildPersistMetadataJson(500, 100, extra);

        Assert.True(MonitoringJobHelper.TryReadPersistMetadata(metadataJson, out var totalRowCount, out var persistedRowCount, out var truncated));
        Assert.Equal(500, totalRowCount);
        Assert.Equal(100, persistedRowCount);
        Assert.True(truncated);
        Assert.True(MonitoringJobHelper.TryGetHasAlertsFromMetadata(metadataJson, out var hasAlerts));
        Assert.True(hasAlerts);

        var columns = MonitoringJobHelper.DeserializeTechnicalRejectColumns(metadataJson);
        Assert.Single(columns);
        Assert.Equal("TradeDate", columns[0].Name);

        using var document = JsonDocument.Parse(metadataJson);
        Assert.True(document.RootElement.TryGetProperty("extra", out _));
    }

    [Fact]
    public void BuildFullResultCsvGzip_RoundTripsRows_AndEscaping()
    {
        var source = new MonitoringTableResult(
            ["Text", "Notes"],
            [
                ["plain", "value"],
                ["comma,value", "quote\"value"],
                ["line\r\nbreak", "two\nlines"]
            ]);

        var payload = MonitoringJobHelper.BuildFullResultCsvGzip(source, CancellationToken.None);
        var csv = Decompress(payload);
        var rows = ParseCsv(csv);

        Assert.Equal(source.Columns.Select(static value => (string?)value), rows[0]);
        Assert.Equal(source.Rows.Count, rows.Count - 1);
        Assert.Equal(source.Rows[0], rows[1]);
        Assert.Equal(source.Rows[1], rows[2]);
        Assert.Equal(source.Rows[2], rows[3]);
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

    private static string Decompress(byte[] payload)
    {
        using var memoryStream = new MemoryStream(payload);
        using var gzipStream = new GZipStream(memoryStream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzipStream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static List<List<string?>> ParseCsv(string csv)
    {
        var rows = new List<List<string?>>();
        var row = new List<string?>();
        var field = new StringBuilder();
        var insideQuotes = false;

        for (var i = 0; i < csv.Length; i++)
        {
            var character = csv[i];
            if (insideQuotes)
            {
                if (character == '"')
                {
                    if (i + 1 < csv.Length && csv[i + 1] == '"')
                    {
                        field.Append('"');
                        i++;
                    }
                    else
                    {
                        insideQuotes = false;
                    }
                }
                else
                {
                    field.Append(character);
                }

                continue;
            }

            switch (character)
            {
                case '"':
                    insideQuotes = true;
                    break;
                case ',':
                    row.Add(field.ToString());
                    field.Clear();
                    break;
                case '\r':
                    if (i + 1 < csv.Length && csv[i + 1] == '\n')
                    {
                        i++;
                    }

                    row.Add(field.ToString());
                    rows.Add(row);
                    row = [];
                    field.Clear();
                    break;
                case '\n':
                    row.Add(field.ToString());
                    rows.Add(row);
                    row = [];
                    field.Clear();
                    break;
                default:
                    field.Append(character);
                    break;
            }
        }

        if (field.Length > 0 || row.Count > 0)
        {
            row.Add(field.ToString());
            rows.Add(row);
        }

        return rows;
    }
}