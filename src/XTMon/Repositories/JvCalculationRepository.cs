using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Data;
using System.Text.Json;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class JvCalculationRepository : IJvCalculationRepository
{
    private const int SlowOperationThresholdMilliseconds = 5_000;

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly JvCalculationOptions _jvCalculationOptions;
    private readonly ILogger<JvCalculationRepository> _logger;

    public JvCalculationRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<JvCalculationOptions> jvCalculationOptions,
        ILogger<JvCalculationRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _jvCalculationOptions = jvCalculationOptions.Value;
        _logger = logger;
    }

    public async Task<JvPnlDatesResult> GetJvPnlDatesAsync(CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.PnlDatesConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.GetPnlDatesStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            var defaultDateParameter = new SqlParameter("@DefaultDate", SqlDbType.Date)
            {
                Direction = ParameterDirection.Output
            };
            command.Parameters.Add(defaultDateParameter);

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var availableDates = new List<DateOnly>();
            while (await reader.ReadAsync(cancellationToken))
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    if (!SqlDataHelper.TryReadDateOnly(reader, i, out var date))
                    {
                        continue;
                    }

                    availableDates.Add(date);
                    break;
                }
            }

            DateOnly? defaultDate = null;
            if (defaultDateParameter.Value is DateTime dateTime)
            {
                defaultDate = DateOnly.FromDateTime(dateTime);
            }
            else if (defaultDateParameter.Value is DateOnly dateOnly)
            {
                defaultDate = dateOnly;
            }
            else if (defaultDateParameter.Value is string rawDate &&
                     DateOnly.TryParse(rawDate, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var parsedDate))
            {
                defaultDate = parsedDate;
            }

            var normalizedDates = availableDates
                .Distinct()
                .OrderByDescending(date => date)
                .ToList();

            if (!defaultDate.HasValue && normalizedDates.Count > 0)
            {
                defaultDate = normalizedDates[0];
            }

            return new JvPnlDatesResult(defaultDate, normalizedDates);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(GetJvPnlDatesAsync), _jvCalculationOptions.PnlDatesConnectionStringName, _jvCalculationOptions.GetPnlDatesStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds);
            throw;
        }
        catch (TimeoutException ex)
        {
            _logger.LogError(AppLogEvents.RepositoryJvSqlTimeout, ex,
                "JV timeout detected for operation {Operation}, connection {ConnectionName}, stored procedure {StoredProcedure}, timeout seconds {TimeoutSeconds}.",
                nameof(GetJvPnlDatesAsync), _jvCalculationOptions.PnlDatesConnectionStringName, _jvCalculationOptions.GetPnlDatesStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV pnl dates procedure failed for {StoredProcedure}.", _jvCalculationOptions.GetPnlDatesStoredProcedure);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(GetJvPnlDatesAsync), _jvCalculationOptions.GetPnlDatesStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<JvCalculationCheckResult> CheckJvCalculationAsync(DateOnly pnlDate, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.PublicationConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.CheckJvCalculationStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            var pnlDateParameter = new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.ToDateTime(TimeOnly.MinValue)
            };
            command.Parameters.Add(pnlDateParameter);

            var executeParameter = new SqlParameter("@Execute", SqlDbType.Bit)
            {
                Value = true
            };
            command.Parameters.Add(executeParameter);

            var queryParameter = new SqlParameter("@Query", SqlDbType.NVarChar, -1)
            {
                Direction = ParameterDirection.Output,
                Value = string.Empty
            };
            command.Parameters.Add(queryParameter);

            var precisionParameter = new SqlParameter("@Precision", SqlDbType.Decimal)
            {
                Precision = 5,
                Scale = 2,
                Value = Math.Round(_jvCalculationOptions.Precision, 2, MidpointRounding.AwayFromZero)
            };
            command.Parameters.Add(precisionParameter);

            await connection.OpenAsync(cancellationToken);
            MonitoringTableResult table;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                table = await MonitoringRepository.ReadMonitoringTableAsync(reader, cancellationToken);

                while (await reader.NextResultAsync(cancellationToken))
                {
                }
            }

            var parsedQuery = SqlDataHelper.ParseQuery(queryParameter.Value);

            return new JvCalculationCheckResult(parsedQuery, table);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(CheckJvCalculationAsync), _jvCalculationOptions.PublicationConnectionStringName, _jvCalculationOptions.CheckJvCalculationStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"PnlDate={pnlDate:yyyy-MM-dd}");
            throw;
        }
        catch (TimeoutException ex)
        {
            _logger.LogError(AppLogEvents.RepositoryJvSqlTimeout, ex,
                "JV timeout detected for operation {Operation}, connection {ConnectionName}, stored procedure {StoredProcedure}, timeout seconds {TimeoutSeconds}, PnlDate {PnlDate}.",
                nameof(CheckJvCalculationAsync), _jvCalculationOptions.PublicationConnectionStringName, _jvCalculationOptions.CheckJvCalculationStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds, pnlDate);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV calculation check procedure failed for {StoredProcedure} and PnlDate {PnlDate}.", _jvCalculationOptions.CheckJvCalculationStoredProcedure, pnlDate);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(CheckJvCalculationAsync), _jvCalculationOptions.CheckJvCalculationStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<string> FixJvCalculationAsync(DateOnly pnlDate, bool executeCatchup, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.PublicationConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.FixJvCalculationStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            var pnlDateParameter = new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.ToDateTime(TimeOnly.MinValue)
            };
            command.Parameters.Add(pnlDateParameter);

            var executeCatchupParameter = new SqlParameter("@ExecuteCatchup", SqlDbType.Bit)
            {
                Value = executeCatchup
            };
            command.Parameters.Add(executeCatchupParameter);

            await connection.OpenAsync(cancellationToken);

            string? rawFixQuery = null;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (true)
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                continue;
                            }

                            var value = reader.GetValue(i);
                            var text = Convert.ToString(value);
                            if (!string.IsNullOrWhiteSpace(text))
                            {
                                rawFixQuery = text;
                                break;
                            }
                        }

                        if (!string.IsNullOrWhiteSpace(rawFixQuery))
                        {
                            break;
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(rawFixQuery))
                    {
                        break;
                    }

                    if (!await reader.NextResultAsync(cancellationToken))
                    {
                        break;
                    }
                }
            }

            return SqlDataHelper.ParseQuery(rawFixQuery);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(FixJvCalculationAsync), _jvCalculationOptions.PublicationConnectionStringName, _jvCalculationOptions.FixJvCalculationStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"PnlDate={pnlDate:yyyy-MM-dd}, ExecuteCatchup={executeCatchup}");
            throw;
        }
        catch (TimeoutException ex)
        {
            _logger.LogError(AppLogEvents.RepositoryJvSqlTimeout, ex,
                "JV timeout detected for operation {Operation}, connection {ConnectionName}, stored procedure {StoredProcedure}, timeout seconds {TimeoutSeconds}, PnlDate {PnlDate}, ExecuteCatchup {ExecuteCatchup}.",
                nameof(FixJvCalculationAsync), _jvCalculationOptions.PublicationConnectionStringName, _jvCalculationOptions.FixJvCalculationStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds, pnlDate, executeCatchup);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV fix procedure failed for {StoredProcedure} and PnlDate {PnlDate}.", _jvCalculationOptions.FixJvCalculationStoredProcedure, pnlDate);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(FixJvCalculationAsync), _jvCalculationOptions.FixJvCalculationStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<JvJobEnqueueResult> EnqueueJvJobAsync(string userId, DateOnly pnlDate, string requestType, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.JobEnqueueStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@UserId", SqlDbType.VarChar, 256) { Value = userId });
            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date) { Value = pnlDate.ToDateTime(TimeOnly.MinValue) });
            command.Parameters.Add(new SqlParameter("@RequestType", SqlDbType.VarChar, 20) { Value = requestType });

            var jobIdParameter = new SqlParameter("@JobId", SqlDbType.BigInt) { Direction = ParameterDirection.Output };
            command.Parameters.Add(jobIdParameter);

            var alreadyActiveParameter = new SqlParameter("@AlreadyActive", SqlDbType.Bit) { Direction = ParameterDirection.Output };
            command.Parameters.Add(alreadyActiveParameter);

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);

            var jobId = Convert.ToInt64(jobIdParameter.Value, System.Globalization.CultureInfo.InvariantCulture);
            var alreadyActive = alreadyActiveParameter.Value is bool boolValue
                ? boolValue
                : Convert.ToInt32(alreadyActiveParameter.Value, System.Globalization.CultureInfo.InvariantCulture) != 0;

            return new JvJobEnqueueResult(jobId, alreadyActive);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(EnqueueJvJobAsync), _jvCalculationOptions.JobConnectionStringName, _jvCalculationOptions.JobEnqueueStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"UserId={userId}, PnlDate={pnlDate:yyyy-MM-dd}, RequestType={requestType}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job enqueue failed for PnlDate {PnlDate} and request type {RequestType}.", pnlDate, requestType);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(EnqueueJvJobAsync), _jvCalculationOptions.JobEnqueueStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<JvJobRecord?> TryTakeNextJvJobAsync(string workerId, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.JobTakeNextStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@WorkerId", SqlDbType.VarChar, 100) { Value = workerId });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ReadJvJobRecord(reader);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(TryTakeNextJvJobAsync), _jvCalculationOptions.JobConnectionStringName, _jvCalculationOptions.JobTakeNextStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"WorkerId={workerId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job take next failed.");
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(TryTakeNextJvJobAsync), _jvCalculationOptions.JobTakeNextStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<JvJobRecord?> GetJvJobByIdAsync(long jobId, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.JobGetByIdStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ReadJvJobRecord(reader);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(GetJvJobByIdAsync), _jvCalculationOptions.JobConnectionStringName, _jvCalculationOptions.JobGetByIdStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job get-by-id failed for JobId {JobId}.", jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(GetJvJobByIdAsync), _jvCalculationOptions.JobGetByIdStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<JvJobRecord?> GetLatestJvJobAsync(string userId, DateOnly pnlDate, string? requestType, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.JobGetLatestStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@UserId", SqlDbType.VarChar, 256) { Value = userId });
            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date) { Value = pnlDate.ToDateTime(TimeOnly.MinValue) });
            command.Parameters.Add(new SqlParameter("@RequestType", SqlDbType.VarChar, 20) { Value = string.IsNullOrWhiteSpace(requestType) ? DBNull.Value : requestType });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return ReadJvJobRecord(reader);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(GetLatestJvJobAsync), _jvCalculationOptions.JobConnectionStringName, _jvCalculationOptions.JobGetLatestStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"UserId={userId}, PnlDate={pnlDate:yyyy-MM-dd}, RequestType={requestType ?? "<null>"}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job latest query failed for user {UserId} and PnlDate {PnlDate}.", userId, pnlDate);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(GetLatestJvJobAsync), _jvCalculationOptions.JobGetLatestStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task SaveJvJobResultAsync(long jobId, string? queryCheck, string? queryFix, MonitoringTableResult? table, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.JobSaveResultStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });
            command.Parameters.Add(new SqlParameter("@QueryCheck", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(queryCheck) ? DBNull.Value : queryCheck });
            command.Parameters.Add(new SqlParameter("@QueryFix", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(queryFix) ? DBNull.Value : queryFix });

            var columnsJson = table is null ? null : JsonSerializer.Serialize(table.Columns);
            var rowsJson = table is null ? null : JsonSerializer.Serialize(table.Rows);
            command.Parameters.Add(new SqlParameter("@GridColumnsJson", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(columnsJson) ? DBNull.Value : columnsJson });
            command.Parameters.Add(new SqlParameter("@GridRowsJson", SqlDbType.NVarChar, -1) { Value = string.IsNullOrWhiteSpace(rowsJson) ? DBNull.Value : rowsJson });

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(SaveJvJobResultAsync), _jvCalculationOptions.JobConnectionStringName, _jvCalculationOptions.JobSaveResultStoredProcedure, _jvCalculationOptions.CommandTimeoutSeconds,
                $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job save result failed for JobId {JobId}.", jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(SaveJvJobResultAsync), _jvCalculationOptions.JobSaveResultStoredProcedure, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task MarkJvJobCompletedAsync(long jobId, CancellationToken cancellationToken)
    {
        await ExecuteJvJobStateProcedureAsync(_jvCalculationOptions.JobMarkCompletedStoredProcedure, jobId, null, cancellationToken);
    }

    public async Task MarkJvJobFailedAsync(long jobId, string errorMessage, CancellationToken cancellationToken)
    {
        await ExecuteJvJobStateProcedureAsync(_jvCalculationOptions.JobMarkFailedStoredProcedure, jobId, errorMessage, cancellationToken);
    }

    public async Task MarkJvJobCancelledAsync(long jobId, string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string commandName = "mark-jv-job-cancelled";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE [monitoring].[JvCalculationJobs]
   SET [Status] = 'Cancelled',
       [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
       [CompletedAt] = NULL,
       [ErrorMessage] = @ErrorMessage,
       [LastHeartbeatAt] = SYSUTCDATETIME()
 WHERE [JobId] = @JobId
   AND [Status] IN ('Running', 'Queued');";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage) ? "JV background job was cancelled by user." : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(MarkJvJobCancelledAsync), _jvCalculationOptions.JobConnectionStringName, commandName, _jvCalculationOptions.CommandTimeoutSeconds,
                $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job cancellation update failed for JobId {JobId}.", jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(MarkJvJobCancelledAsync), commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task HeartbeatJvJobAsync(long jobId, CancellationToken cancellationToken)
    {
        await ExecuteJvJobStateProcedureAsync(_jvCalculationOptions.JobHeartbeatStoredProcedure, jobId, null, cancellationToken);
    }

    public async Task<int> CancelActiveJvJobsAsync(string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(CancelActiveJvJobsAsync);
        const string commandName = "bulk-cancel-active-jv-jobs";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE [monitoring].[JvCalculationJobs]
   SET [Status] = 'Cancelled',
       [FailedAt] = CASE WHEN [StartedAt] IS NULL THEN NULL ELSE SYSUTCDATETIME() END,
       [CompletedAt] = NULL,
       [ErrorMessage] = @ErrorMessage,
       [LastHeartbeatAt] = SYSUTCDATETIME()
 WHERE [Status] IN ('Queued', 'Running');";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage)
                    ? "JV background jobs were cancelled by user."
                    : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, _jvCalculationOptions.JobConnectionStringName, commandName, _jvCalculationOptions.CommandTimeoutSeconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV bulk cancellation failed while failing active jobs.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<int> CountActiveJvJobsAsync(CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(CountActiveJvJobsAsync);
        const string commandName = "count-active-jv-jobs";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT COUNT_BIG(*)
FROM [monitoring].[JvCalculationJobs]
WHERE [Status] IN ('Queued', 'Running');";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            await connection.OpenAsync(cancellationToken);
            var count = await command.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt32(count, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, _jvCalculationOptions.JobConnectionStringName, commandName, _jvCalculationOptions.CommandTimeoutSeconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV active job count query failed.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    public async Task<int> ExpireStaleRunningJobsAsync(TimeSpan staleAfter, string errorMessage, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _jvCalculationOptions.JobExpireStaleStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@StaleTimeoutSeconds", SqlDbType.Int)
            {
                Value = Math.Max(1, Convert.ToInt32(staleAfter.TotalSeconds, System.Globalization.CultureInfo.InvariantCulture))
            });

            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage)
                    ? "JV background job timed out while in Running status."
                    : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV stale job expiration failed.");
            throw;
        }
    }

    public async Task<int> FailRunningJvJobsAsync(string errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        const string operationName = nameof(FailRunningJvJobsAsync);
        const string commandName = "startup-running-jv-job-recovery";

        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE [monitoring].[JvCalculationJobs]
   SET [Status] = 'Failed',
       [FailedAt] = SYSUTCDATETIME(),
       [CompletedAt] = NULL,
       [ErrorMessage] = @ErrorMessage,
       [LastHeartbeatAt] = SYSUTCDATETIME()
 WHERE [Status] = 'Running';";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
            {
                Value = string.IsNullOrWhiteSpace(errorMessage)
                    ? "JV background job was failed during startup recovery."
                    : errorMessage
            });

            await connection.OpenAsync(cancellationToken);
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName, _jvCalculationOptions.JobConnectionStringName, commandName, _jvCalculationOptions.CommandTimeoutSeconds);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV startup recovery failed while failing running jobs.");
            throw;
        }
        finally
        {
            LogOperationDuration(operationName, commandName, stopwatch.ElapsedMilliseconds);
        }
    }

    private async Task ExecuteJvJobStateProcedureAsync(string procedureName, long jobId, string? errorMessage, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_jvCalculationOptions.JobConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = procedureName;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _jvCalculationOptions.CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt) { Value = jobId });

            if (errorMessage is not null)
            {
                command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1)
                {
                    Value = string.IsNullOrWhiteSpace(errorMessage) ? "Unknown JV job failure." : errorMessage
                });
            }

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, operationName: nameof(ExecuteJvJobStateProcedureAsync), _jvCalculationOptions.JobConnectionStringName, procedureName, _jvCalculationOptions.CommandTimeoutSeconds,
                $"JobId={jobId}");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "JV job state update failed for procedure {StoredProcedure} and JobId {JobId}.", procedureName, jobId);
            throw;
        }
        finally
        {
            LogOperationDuration(nameof(ExecuteJvJobStateProcedureAsync), procedureName, stopwatch.ElapsedMilliseconds);
        }
    }

    private void LogOperationDuration(string operationName, string commandName, long elapsedMilliseconds)
    {
        if (elapsedMilliseconds >= SlowOperationThresholdMilliseconds)
        {
            _logger.LogWarning(AppLogEvents.RepositoryJvSlowOperation,
                "JV data operation {Operation} with command {CommandName} is slow. ElapsedMs={ElapsedMs}.",
                operationName, commandName, elapsedMilliseconds);
        }
        else
        {
            _logger.LogDebug(
                "JV data operation {Operation} with command {CommandName} completed in {ElapsedMs} ms.",
                operationName, commandName, elapsedMilliseconds);
        }
    }

    private void LogSqlException(
        SqlException ex,
        string operationName,
        string connectionName,
        string commandName,
        int timeoutSeconds,
        string? context = null)
    {
        if (SqlDataHelper.IsSqlTimeout(ex))
        {
            _logger.LogError(AppLogEvents.RepositoryJvSqlTimeout, ex,
                "JV SQL timeout in operation {Operation}, connection {ConnectionName}, command {CommandName}, timeout seconds {TimeoutSeconds}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                connectionName,
                commandName,
                timeoutSeconds,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        if (SqlDataHelper.IsSqlConnectionFailure(ex))
        {
            _logger.LogError(AppLogEvents.RepositoryJvConnectionFailed, ex,
                "JV SQL connection problem in operation {Operation}, connection {ConnectionName}, command {CommandName}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
                operationName,
                connectionName,
                commandName,
                ex.Number,
                ex.State,
                ex.Class,
                context ?? "N/A");
            return;
        }

        _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex,
            "JV SQL error in operation {Operation}, connection {ConnectionName}, command {CommandName}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. Context: {Context}.",
            operationName,
            connectionName,
            commandName,
            ex.Number,
            ex.State,
            ex.Class,
            context ?? "N/A");
    }

    private static JvJobRecord ReadJvJobRecord(IDataRecord reader)
    {
        var jobId = Convert.ToInt64(reader["JobId"], System.Globalization.CultureInfo.InvariantCulture);
        var userId = Convert.ToString(reader["UserId"], System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
        var pnlDate = DateOnly.FromDateTime(Convert.ToDateTime(reader["PnlDate"], System.Globalization.CultureInfo.InvariantCulture));
        var requestType = Convert.ToString(reader["RequestType"], System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
        var status = Convert.ToString(reader["Status"], System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
        var workerId = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "WorkerId"));
        var enqueuedAt = Convert.ToDateTime(reader["EnqueuedAt"], System.Globalization.CultureInfo.InvariantCulture);
        var startedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "StartedAt"));
        var heartbeatAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "LastHeartbeatAt"));
        var completedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "CompletedAt"));
        var failedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "FailedAt"));
        var errorMessage = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "ErrorMessage"));
        var queryCheck = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "QueryCheck"));
        var queryFix = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "QueryFix"));
        var gridColumnsJson = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "GridColumnsJson"));
        var gridRowsJson = SqlDataHelper.ReadNullableString(reader, SqlDataHelper.FindOrdinal(reader, "GridRowsJson"));
        var savedAt = SqlDataHelper.ReadNullableDateTime(reader, SqlDataHelper.FindOrdinal(reader, "SavedAt"));

        return new JvJobRecord(
            jobId,
            userId,
            pnlDate,
            requestType,
            status,
            workerId,
            enqueuedAt,
            startedAt,
            heartbeatAt,
            completedAt,
            failedAt,
            errorMessage,
            queryCheck,
            queryFix,
            gridColumnsJson,
            gridRowsJson,
            savedAt);
    }
}
