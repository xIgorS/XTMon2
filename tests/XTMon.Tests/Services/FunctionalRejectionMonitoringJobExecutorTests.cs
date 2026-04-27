using System.IO.Compression;
using Microsoft.Extensions.Options;
using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class FunctionalRejectionMonitoringJobExecutorTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    [Fact]
    public async Task ExecuteAsync_TruncatesPreview_AndPersistsFullCsvBlob()
    {
        var repository = new Mock<IFunctionalRejectionRepository>(MockBehavior.Strict);
        repository
            .Setup(mock => mock.GetTechnicalRejectAsync(TestDate, 42, "STAGING", "SYS", It.IsAny<CancellationToken>()))
            .ReturnsAsync(BuildResult(rowCount: 500));

        var executor = new FunctionalRejectionMonitoringJobExecutor(
            repository.Object,
            Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions { MaxPersistedRows = 100 }));

        var payload = await executor.ExecuteAsync(MakeJob(), CancellationToken.None);

        Assert.NotNull(payload.Table);
        Assert.Equal(100, payload.Table!.Rows.Count);
        Assert.NotNull(payload.FullResultCsvGzip);
        Assert.True(MonitoringJobHelper.TryReadPersistMetadata(payload.MetadataJson, out var totalRowCount, out var persistedRowCount, out var truncated));
        Assert.Equal(500, totalRowCount);
        Assert.Equal(100, persistedRowCount);
        Assert.True(truncated);
        Assert.Equal(501, CountCsvLines(payload.FullResultCsvGzip!));

        var columns = MonitoringJobHelper.DeserializeTechnicalRejectColumns(payload.MetadataJson);
        Assert.Equal(2, columns.Count);
        Assert.Equal("TradeDate", columns[0].Name);
        Assert.True(MonitoringJobHelper.TryGetHasAlertsFromMetadata(payload.MetadataJson, out var hasAlerts));
        Assert.True(hasAlerts);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotTruncate_WhenMaxPersistedRowsIsZero()
    {
        var repository = new Mock<IFunctionalRejectionRepository>(MockBehavior.Strict);
        repository
            .Setup(mock => mock.GetTechnicalRejectAsync(TestDate, 42, "STAGING", "SYS", It.IsAny<CancellationToken>()))
            .ReturnsAsync(BuildResult(rowCount: 5));

        var executor = new FunctionalRejectionMonitoringJobExecutor(
            repository.Object,
            Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions { MaxPersistedRows = 0 }));

        var payload = await executor.ExecuteAsync(MakeJob(), CancellationToken.None);

        Assert.NotNull(payload.Table);
        Assert.Equal(5, payload.Table!.Rows.Count);
        Assert.NotNull(payload.FullResultCsvGzip);
        Assert.True(MonitoringJobHelper.TryReadPersistMetadata(payload.MetadataJson, out var totalRowCount, out var persistedRowCount, out var truncated));
        Assert.Equal(5, totalRowCount);
        Assert.Equal(5, persistedRowCount);
        Assert.False(truncated);
        Assert.Equal(6, CountCsvLines(payload.FullResultCsvGzip!));
    }

    private static MonitoringJobRecord MakeJob()
    {
        return new MonitoringJobRecord(
            JobId: 1L,
            Category: MonitoringJobHelper.FunctionalRejectionCategory,
            SubmenuKey: MonitoringJobHelper.BuildFunctionalRejectionSubmenuKey(42, "SYS", "STAGING", "CODE"),
            DisplayName: "Functional Rejection",
            PnlDate: TestDate,
            Status: MonitoringJobHelper.QueuedStatus,
            WorkerId: null,
            ParametersJson: MonitoringJobHelper.SerializeParameters(new FunctionalRejectionJobParameters("CODE", 42, "SYS", "STAGING")),
            ParameterSummary: null,
            EnqueuedAt: DateTime.UtcNow,
            StartedAt: null,
            LastHeartbeatAt: null,
            CompletedAt: null,
            FailedAt: null,
            ErrorMessage: null,
            ParsedQuery: null,
            GridColumnsJson: null,
            GridRowsJson: null,
            MetadataJson: null,
            SavedAt: null);
    }

    private static TechnicalRejectResult BuildResult(int rowCount)
    {
        return new TechnicalRejectResult(
            ParsedQuery: "SELECT 1",
            Columns:
            [
                new TechnicalRejectColumn("TradeDate", "date"),
                new TechnicalRejectColumn("Amount", "decimal")
            ],
            Rows: Enumerable.Range(1, rowCount)
                .Select(index => (IReadOnlyList<string?>)["2026-01-15", index.ToString()])
                .ToArray());
    }

    private static int CountCsvLines(byte[] payload)
    {
        using var memoryStream = new MemoryStream(payload);
        using var gzipStream = new GZipStream(memoryStream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzipStream);
        var csv = reader.ReadToEnd();
        return csv.Split("\r\n", StringSplitOptions.RemoveEmptyEntries).Length;
    }
}