using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class MonitoringJobRepository : IMonitoringJobRepository
{
    private const long SlowOperationThresholdMilliseconds = 1000;

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly MonitoringJobsOptions _options;
    private readonly ILogger<MonitoringJobRepository> _logger;

    public MonitoringJobRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<MonitoringJobsOptions> options,
        ILogger<MonitoringJobRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<MonitoringJobEnqueueResult> EnqueueMonitoringJobAsync(
        string category,
        string submenuKey,
        string? displayName,
        DateOnly pnlDate,
        string? parametersJson,
        string? parameterSummary,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobEnqueueStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@Category", SqlDbType.VarChar, 64) { Value = category });
            command.Parameters.Add(new SqlParameter("@SubmenuKey", SqlDbType.NVarChar, 512) { Value = submenuKey });
            command.Parameters.Add(new SqlParameter("@DisplayName", SqlDbType.NVarChar, 256) { Value = string.IsNullOrWhiteSpace(displayName) ? DBNull.Value : displayName });
            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date) { Value = pnlDate.ToDateTime(TimeOnly.MinValue) });
            command.Parameters.Add(new SqlParameter("@ParametersJson", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(parametersJson) ? DBNull.Value : parametersJson });
            command.Parameters.Add(new SqlParameter("@ParameterSummary", SqlDbType.NVarChar, 1024) { Value = string.IsNullOrWhiteSpace(parameterSummary) ? DBNull.Value : parameterSummary });

            var jobIdParameter = new SqlParameter("@JobId", SqlDbType.BigInt) { Direction = ParameterDirection.Output };
            command.Parameters.Add(jobIdParameter);

            var alreadyActiveParameter = new SqlParameter("@AlreadyActive", SqlDbType.Bit) { Direction = ParameterDirection.Output };
            command.Parameters.Add(alreadyActiveParameter);

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);

            var jobId = Convert.ToInt64(jobIdParameter.Value, CultureInfo.InvariantCulture);
            var alreadyActive = alreadyActiveParameter.Value is bool boolValue
                ? boolValue
                : Convert.ToInt32(alreadyActiveParameter.Value, CultureInfo.InvariantCulture) != 0;

            return new MonitoringJobEnqueueResult(jobId, alreadyActive);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(EnqueueMonitoringJobAsync), _options.JobEnqueueStoredProcedure, stopwatch.ElapsedMilliseconds,
                $"Category={category}, SubmenuKey={submenuKey}, PnlDate={pnlDate:yyyy-MM-dd}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
                "Monitoring job enqueue failed for category {Category}, submenu {SubmenuKey}, pnl date {PnlDate}.",
                category,
                submenuKey,
                pnlDate);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(EnqueueMonitoringJobAsync), _options.JobEnqueueStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<MonitoringJobRecord?> TryTakeNextMonitoringJobAsync(string workerId, CancellationToken cancellationToken)
    {
        return await TryTakeNextMonitoringJobAsync(workerId, excludedCategories: null, cancellationToken);
    }

    public async Task<MonitoringJobRecord?> TryTakeNextMonitoringJobAsync(string workerId, IReadOnlyCollection<string>? excludedCategories, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobTakeNextStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@WorkerId", SqlDbType.VarChar, 100) { Value = workerId });
            command.Parameters.Add(new SqlParameter("@ExcludedCategoriesCsv", SqlDbType.NVarChar, 4000)
            {
                Value = BuildExcludedCategoriesCsv(excludedCategories)
            });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ReadMonitoringJobRecord(reader);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(TryTakeNextMonitoringJobAsync), _options.JobTakeNextStoredProcedure, stopwatch.ElapsedMilliseconds,
                $"WorkerId={workerId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring job take next failed.");
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(TryTakeNextMonitoringJobAsync), _options.JobTakeNextStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    private static object BuildExcludedCategoriesCsv(IReadOnlyCollection<string>? excludedCategories)
    {
        if (excludedCategories is null || excludedCategories.Count == 0)
        {
            return DBNull.Value;
        }

        var categories = excludedCategories
            .Where(category => !string.IsNullOrWhiteSpace(category))
            .Select(category => category.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        return categories.Length == 0
            ? DBNull.Value
            : string.Join(',', categories);
    }

    public async Task<MonitoringJobRecord?> GetMonitoringJobByIdAsync(long jobId, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobGetByIdStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ReadMonitoringJobRecord(reader);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(GetMonitoringJobByIdAsync), _options.JobGetByIdStoredProcedure, stopwatch.ElapsedMilliseconds,
                $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring job get-by-id failed for JobId {JobId}.", jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(GetMonitoringJobByIdAsync), _options.JobGetByIdStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<MonitoringJobRecord?> GetLatestMonitoringJobAsync(string category, string submenuKey, DateOnly pnlDate, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobGetLatestStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@Category", SqlDbType.VarChar, 64) { Value = category });
            command.Parameters.Add(new SqlParameter("@SubmenuKey", SqlDbType.NVarChar, 512) { Value = submenuKey });
            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date) { Value = pnlDate.ToDateTime(TimeOnly.MinValue) });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ReadMonitoringJobRecord(reader);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(GetLatestMonitoringJobAsync), _options.JobGetLatestStoredProcedure, stopwatch.ElapsedMilliseconds,
                $"Category={category}, SubmenuKey={submenuKey}, PnlDate={pnlDate:yyyy-MM-dd}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
                "Monitoring latest job query failed for category {Category}, submenu {SubmenuKey}, pnl date {PnlDate}.",
                category,
                submenuKey,
                pnlDate);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(GetLatestMonitoringJobAsync), _options.JobGetLatestStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<IReadOnlyList<MonitoringJobRecord>> GetLatestMonitoringJobsByCategoryAsync(string category, DateOnly pnlDate, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobGetLatestByCategoryStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@Category", SqlDbType.VarChar, 64) { Value = category });
            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date) { Value = pnlDate.ToDateTime(TimeOnly.MinValue) });

            var jobs = new List<MonitoringJobRecord>();

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                jobs.Add(ReadMonitoringJobRecord(reader));
            }

            return jobs;
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(GetLatestMonitoringJobsByCategoryAsync), _options.JobGetLatestByCategoryStoredProcedure, stopwatch.ElapsedMilliseconds,
                $"Category={category}, PnlDate={pnlDate:yyyy-MM-dd}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
                "Monitoring latest jobs query failed for category {Category}, pnl date {PnlDate}.",
                category,
                pnlDate);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(GetLatestMonitoringJobsByCategoryAsync), _options.JobGetLatestByCategoryStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task SaveMonitoringJobResultAsync(long jobId, MonitoringJobResultPayload payload, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobSaveResultStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            var columnsJson = payload.Table is null ? null : JsonSerializer.Serialize(payload.Table.Columns);
            var rowsJson = payload.Table is null ? null : JsonSerializer.Serialize(payload.Table.Rows);

            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });
            command.Parameters.Add(new SqlParameter("@ParsedQuery", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(payload.ParsedQuery) ? DBNull.Value : payload.ParsedQuery });
            command.Parameters.Add(new SqlParameter("@GridColumnsJson", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(columnsJson) ? DBNull.Value : columnsJson });
            command.Parameters.Add(new SqlParameter("@GridRowsJson", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(rowsJson) ? DBNull.Value : rowsJson });
            command.Parameters.Add(new SqlParameter("@MetadataJson", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(payload.MetadataJson) ? DBNull.Value : payload.MetadataJson });

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(SaveMonitoringJobResultAsync), _options.JobSaveResultStoredProcedure, stopwatch.ElapsedMilliseconds,
                $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring job save result failed for JobId {JobId}.", jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(SaveMonitoringJobResultAsync), _options.JobSaveResultStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public Task MarkMonitoringJobCompletedAsync(long jobId, CancellationToken cancellationToken)
    {
        return ExecuteMonitoringJobStateProcedureAsync(_options.JobMarkCompletedStoredProcedure, jobId, null, cancellationToken);
    }

    public Task MarkMonitoringJobFailedAsync(long jobId, string errorMessage, CancellationToken cancellationToken)
    {
        return ExecuteMonitoringJobStateProcedureAsync(_options.JobMarkFailedStoredProcedure, jobId, errorMessage, cancellationToken);
    }

    public async Task MarkMonitoringJobCancelledAsync(long jobId, string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string commandName = "mark-monitoring-job-cancelled";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE [monitoring].[MonitoringJobs]
   SET [Status] = 'Cancelled',
       [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
       [CompletedAt] = NULL,
       [ErrorMessage] = @ErrorMessage,
       [LastHeartbeatAt] = SYSUTCDATETIME()
 WHERE [JobId] = @JobId
   AND [Status] IN ('Running', 'Queued');";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage) ? "Monitoring background job was cancelled by user." : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, nameof(MarkMonitoringJobCancelledAsync), commandName, stopwatch.ElapsedMilliseconds, $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring job cancellation update failed for JobId {JobId}.", jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(MarkMonitoringJobCancelledAsync), commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public Task HeartbeatMonitoringJobAsync(long jobId, CancellationToken cancellationToken)
    {
        return ExecuteMonitoringJobStateProcedureAsync(_options.JobHeartbeatStoredProcedure, jobId, null, cancellationToken);
    }

    public async Task<int> CancelActiveMonitoringJobsAsync(string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(CancelActiveMonitoringJobsAsync);
        const string commandName = "bulk-cancel-active-monitoring-jobs";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE [monitoring].[MonitoringJobs]
   SET [Status] = 'Cancelled',
       [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
       [CompletedAt] = NULL,
       [ErrorMessage] = @ErrorMessage,
       [LastHeartbeatAt] = SYSUTCDATETIME()
 WHERE [Status] IN ('Queued', 'Running');";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage)
                    ? "Monitoring background jobs were cancelled by user."
                    : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, commandName, stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring bulk cancellation failed while failing active jobs.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<int> CountActiveMonitoringJobsAsync(CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(CountActiveMonitoringJobsAsync);
        const string commandName = "count-active-monitoring-jobs";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT COUNT_BIG(*)
FROM [monitoring].[MonitoringJobs]
WHERE [Status] IN ('Queued', 'Running');";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            await connection.OpenAsync(cancellationToken);
            var count = await command.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt32(count, CultureInfo.InvariantCulture);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, commandName, stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring active job count query failed.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<int> FailRunningMonitoringJobsAsync(string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(FailRunningMonitoringJobsAsync);
        const string commandName = "startup-running-monitoring-job-recovery";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE [monitoring].[MonitoringJobs]
   SET [Status] = 'Failed',
       [FailedAt] = SYSUTCDATETIME(),
       [CompletedAt] = NULL,
       [ErrorMessage] = @ErrorMessage,
       [LastHeartbeatAt] = SYSUTCDATETIME()
 WHERE [Status] = 'Running';";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage)
                    ? "Monitoring background job was failed during startup recovery."
                    : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, commandName, stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring startup recovery failed while failing running jobs.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<IReadOnlyList<MonitoringJobRecord>> GetStuckMonitoringJobsAsync(TimeSpan activityOlderThan, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(GetStuckMonitoringJobsAsync);
        const string commandName = "get-stuck-monitoring-jobs";
        var thresholdSeconds = Math.Max(1, Convert.ToInt32(activityOlderThan.TotalSeconds, CultureInfo.InvariantCulture));

        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT [JobId], [Category], [SubmenuKey], [DisplayName], [PnlDate], [Status], [WorkerId],
       [ParametersJson], [ParameterSummary], [EnqueuedAt], [StartedAt], [LastHeartbeatAt],
       [CompletedAt], [FailedAt], [ErrorMessage],
       CAST(NULL AS NVARCHAR(MAX)) AS [ParsedQuery],
       CAST(NULL AS NVARCHAR(MAX)) AS [GridColumnsJson],
       CAST(NULL AS NVARCHAR(MAX)) AS [GridRowsJson],
       CAST(NULL AS NVARCHAR(MAX)) AS [MetadataJson],
       CAST(NULL AS DATETIME2(0))  AS [SavedAt]
  FROM [monitoring].[MonitoringJobs]
 WHERE [Status] = 'Running'
   AND DATEDIFF(SECOND, [ActivityAt], SYSUTCDATETIME()) > @ThresholdSeconds
 ORDER BY [ActivityAt];";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@ThresholdSeconds", SqlDbType.Int) { Value = thresholdSeconds });

            var jobs = new List<MonitoringJobRecord>();

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                jobs.Add(ReadMonitoringJobRecord(reader));
            }

            return jobs;
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, commandName, stopwatch.ElapsedMilliseconds, $"ThresholdSeconds={thresholdSeconds}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring stuck-job query failed.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<int> ExpireStaleRunningMonitoringJobsAsync(TimeSpan staleAfter, string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var staleTimeoutSeconds = Math.Max(1, Convert.ToInt32(staleAfter.TotalSeconds, CultureInfo.InvariantCulture));

        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.JobExpireStaleStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@StaleTimeoutSeconds", SqlDbType.Int)
            {
                Value = staleTimeoutSeconds
            });
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage)
                    ? "Monitoring background job timed out while in Running status."
                    : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(
                ex,
                nameof(ExpireStaleRunningMonitoringJobsAsync),
                _options.JobExpireStaleStoredProcedure,
                stopwatch.ElapsedMilliseconds,
                $"StaleTimeoutSeconds={staleTimeoutSeconds}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring stale job expiration failed.");
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(ExpireStaleRunningMonitoringJobsAsync), _options.JobExpireStaleStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    private async Task ExecuteMonitoringJobStateProcedureAsync(string procedureName, long jobId, string? errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var suppressSlowOperationWarning = false;
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = procedureName;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });

            if (errorMessage is not null)
            {
                command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
                {
                    Value = string.IsNullOrWhiteSpace(errorMessage) ? "Unknown monitoring job failure." : errorMessage
                });
            }

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            suppressSlowOperationWarning = ShouldSuppressHeartbeatSlowWarning(procedureName, ex);
            LogSqlException(ex, nameof(ExecuteMonitoringJobStateProcedureAsync), procedureName, stopwatch.ElapsedMilliseconds, $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
                "Monitoring job state update failed for procedure {StoredProcedure} and JobId {JobId}.",
                procedureName,
                jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(ExecuteMonitoringJobStateProcedureAsync), procedureName, stopwatch.ElapsedMilliseconds, suppressSlowOperationWarning);
        }
    }

    private void LogOperationDuration(string operationName, string commandName, long elapsedMilliseconds, bool suppressWarning = false)
    {
        if (suppressWarning)
        {
            _logger.LogDebug(
                "Monitoring job data operation {Operation} with command {CommandName} completed in {ElapsedMs} ms.",
                operationName,
                commandName,
                elapsedMilliseconds);
            return;
        }

        if (elapsedMilliseconds >= SlowOperationThresholdMilliseconds)
        {
            _logger.LogWarning(AppLogEvents.RepositoryMonitoringJobSlowOperation,
                "Monitoring job data operation {Operation} with command {CommandName} is slow. ElapsedMs={ElapsedMs}.",
                operationName,
                commandName,
                elapsedMilliseconds);
        }
        else
        {
            _logger.LogDebug(
                "Monitoring job data operation {Operation} with command {CommandName} completed in {ElapsedMs} ms.",
                operationName,
                commandName,
                elapsedMilliseconds);
        }
    }

    private void LogSqlException(SqlException ex, string operationName, string commandName, long elapsedMilliseconds, string? context = null)
    {
        if (IsTransientHeartbeatFailure(commandName, ex))
        {
            _logger.LogInformation(
                "Monitoring heartbeat transient SQL failure in operation {Operation}, connection {ConnectionName}, command {CommandName}, elapsed ms {ElapsedMs}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                _options.JobConnectionStringName,
                commandName,
                elapsedMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        if (SqlDataHelper.IsSqlTimeout(ex))
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringJobSqlTimeout, ex,
                "Monitoring job SQL timeout in operation {Operation}, connection {ConnectionName}, command {CommandName}, timeout seconds {TimeoutSeconds}, elapsed ms {ElapsedMs}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                _options.JobConnectionStringName,
                commandName,
                _options.CommandTimeoutSeconds,
                elapsedMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        if (SqlDataHelper.IsSqlLockTimeout(ex))
        {
            _logger.LogWarning(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
                "Monitoring job SQL lock timeout in operation {Operation}, connection {ConnectionName}, command {CommandName}, elapsed ms {ElapsedMs}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                _options.JobConnectionStringName,
                commandName,
                elapsedMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        if (SqlDataHelper.IsSqlConnectionFailure(ex))
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringJobConnectionFailed, ex,
                "Monitoring job SQL connection problem in operation {Operation}, connection {ConnectionName}, command {CommandName}, elapsed ms {ElapsedMs}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                _options.JobConnectionStringName,
                commandName,
                elapsedMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        if (SqlDataHelper.IsSqlDeadlock(ex))
        {
            _logger.LogWarning(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
                "Monitoring job SQL deadlock in operation {Operation}, connection {ConnectionName}, command {CommandName}, elapsed ms {ElapsedMs}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                _options.JobConnectionStringName,
                commandName,
                elapsedMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
            "Monitoring job SQL error in operation {Operation}, connection {ConnectionName}, command {CommandName}, elapsed ms {ElapsedMs}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
            operationName,
            _options.JobConnectionStringName,
            commandName,
            elapsedMilliseconds,
            ex.Number,
            ex.State,
            ex.Class,
            context ?? "N/A");
    }

    private bool IsTransientHeartbeatFailure(string commandName, SqlException ex)
    {
        return string.Equals(commandName, _options.JobHeartbeatStoredProcedure, StringComparison.OrdinalIgnoreCase)
            && (SqlDataHelper.IsSqlTimeout(ex)
                || SqlDataHelper.IsSqlLockTimeout(ex)
                || SqlDataHelper.IsSqlDeadlock(ex)
                || SqlDataHelper.IsSqlConnectionFailure(ex));
    }

    private bool ShouldSuppressHeartbeatSlowWarning(string commandName, SqlException ex)
    {
        return IsTransientHeartbeatFailure(commandName, ex);
    }

    private static MonitoringJobRecord ReadMonitoringJobRecord(IDataRecord reader)
    {
        var jobId = Convert.ToInt64(reader["JobId"], CultureInfo.InvariantCulture);
        var category = Convert.ToString(reader["Category"], CultureInfo.InvariantCulture) ?? string.Empty;
        var submenuKey = Convert.ToString(reader["SubmenuKey"], CultureInfo.InvariantCulture) ?? string.Empty;
        var displayName = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "DisplayName"));
        var pnlDate = DateOnly.FromDateTime(Convert.ToDateTime(reader["PnlDate"], CultureInfo.InvariantCulture));
        var status = Convert.ToString(reader["Status"], CultureInfo.InvariantCulture) ?? string.Empty;
        var workerId = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "WorkerId"));
        var parametersJson = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "ParametersJson"));
        var parameterSummary = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "ParameterSummary"));
        var enqueuedAt = Convert.ToDateTime(reader["EnqueuedAt"], CultureInfo.InvariantCulture);
        var startedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "StartedAt"));
        var heartbeatAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "LastHeartbeatAt"));
        var completedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "CompletedAt"));
        var failedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "FailedAt"));
        var errorMessage = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "ErrorMessage"));
        var parsedQuery = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "ParsedQuery"));
        var gridColumnsJson = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "GridColumnsJson"));
        var gridRowsJson = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "GridRowsJson"));
        var metadataJson = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "MetadataJson"));
        var savedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "SavedAt"));

        return new MonitoringJobRecord(
            jobId,
            category,
            submenuKey,
            displayName,
            pnlDate,
            status,
            workerId,
            parametersJson,
            parameterSummary,
            enqueuedAt,
            startedAt,
            heartbeatAt,
            completedAt,
            failedAt,
            errorMessage,
            parsedQuery,
            gridColumnsJson,
            gridRowsJson,
            metadataJson,
            savedAt);
    }
}