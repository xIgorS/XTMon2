using System.IO.Compression;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using XTMon.Helpers;
using XTMon.Models;
using XTMon.Options;
using XTMon.Repositories;
using XTMon.Services;

namespace XTMon.Tests.Services;

public class DataValidationMonitoringJobExecutorTests
{
    private static readonly DateOnly TestDate = new(2026, 1, 15);

    [Fact]
    public void CanExecute_ReturnsFalse_ForBatchStatus()
    {
        var executor = new DataValidationMonitoringJobExecutor(Mock.Of<IServiceProvider>());
        var job = MakeJob(MonitoringJobHelper.BatchStatusSubmenuKey);

        var canExecute = executor.CanExecute(job);

        Assert.False(canExecute);
    }

    [Fact]
    public void CanExecute_ReturnsTrue_ForOwnedDataValidationSubmenu()
    {
        var executor = new DataValidationMonitoringJobExecutor(Mock.Of<IServiceProvider>());
        var job = MakeJob("daily-balance");

        var canExecute = executor.CanExecute(job);

        Assert.True(canExecute);
    }

    [Fact]
    public async Task ExecuteAsync_TruncatesPreview_AndPersistsFullCsvBlob()
    {
        var fullTable = BuildTable(rowCount: 500);
        var repository = new Mock<IMarketDataRepository>(MockBehavior.Strict);
        repository
            .Setup(mock => mock.GetMarketDataAsync(TestDate, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MarketDataResult("SELECT 1", fullTable));

        using var serviceProvider = BuildServiceProvider(repository.Object, maxPersistedRows: 100);
        var executor = new DataValidationMonitoringJobExecutor(serviceProvider);

        var payload = await executor.ExecuteAsync(MakeJob("market-data"), CancellationToken.None);

        Assert.NotNull(payload.Table);
        Assert.Equal(100, payload.Table!.Rows.Count);
        Assert.NotNull(payload.FullResultCsvGzip);
        Assert.True(MonitoringJobHelper.TryReadPersistMetadata(payload.MetadataJson, out var totalRowCount, out var persistedRowCount, out var truncated));
        Assert.Equal(500, totalRowCount);
        Assert.Equal(100, persistedRowCount);
        Assert.True(truncated);
        Assert.Equal(501, CountCsvLines(payload.FullResultCsvGzip!));
        Assert.True(MonitoringJobHelper.TryGetHasAlertsFromMetadata(payload.MetadataJson, out var hasAlerts));
        Assert.False(hasAlerts);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotTruncate_WhenMaxPersistedRowsIsZero()
    {
        var fullTable = BuildTable(rowCount: 5);
        var repository = new Mock<IMarketDataRepository>(MockBehavior.Strict);
        repository
            .Setup(mock => mock.GetMarketDataAsync(TestDate, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MarketDataResult("SELECT 1", fullTable));

        using var serviceProvider = BuildServiceProvider(repository.Object, maxPersistedRows: 0);
        var executor = new DataValidationMonitoringJobExecutor(serviceProvider);

        var payload = await executor.ExecuteAsync(MakeJob("market-data"), CancellationToken.None);

        Assert.Same(fullTable, payload.Table);
        Assert.NotNull(payload.FullResultCsvGzip);
        Assert.True(MonitoringJobHelper.TryReadPersistMetadata(payload.MetadataJson, out var totalRowCount, out var persistedRowCount, out var truncated));
        Assert.Equal(5, totalRowCount);
        Assert.Equal(5, persistedRowCount);
        Assert.False(truncated);
        Assert.Equal(6, CountCsvLines(payload.FullResultCsvGzip!));
    }

    private static MonitoringJobRecord MakeJob(string submenuKey)
    {
        return new MonitoringJobRecord(
            JobId: 1L,
            Category: MonitoringJobHelper.DataValidationCategory,
            SubmenuKey: submenuKey,
            DisplayName: submenuKey,
            PnlDate: TestDate,
            Status: MonitoringJobHelper.QueuedStatus,
            WorkerId: null,
            ParametersJson: null,
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

    private static MonitoringTableResult BuildTable(int rowCount)
    {
        return new MonitoringTableResult(
            ["Status", "Value"],
            Enumerable.Range(1, rowCount)
                .Select(index => (IReadOnlyList<string?>)["OK", index.ToString()])
                .ToArray());
    }

    private static ServiceProvider BuildServiceProvider(IMarketDataRepository repository, int maxPersistedRows)
    {
        return new ServiceCollection()
            .AddSingleton(repository)
            .AddSingleton<IOptions<MonitoringJobsOptions>>(Microsoft.Extensions.Options.Options.Create(new MonitoringJobsOptions
            {
                MaxPersistedRows = maxPersistedRows
            }))
            .BuildServiceProvider();
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