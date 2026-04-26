using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class ApplicationLogsRepository : IApplicationLogsRepository
{
    private const int SlowOperationThresholdMilliseconds = 5_000;

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly ApplicationLogsOptions _options;
    private readonly ILogger<ApplicationLogsRepository> _logger;

    public ApplicationLogsRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<ApplicationLogsOptions> options,
        ILogger<ApplicationLogsRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<ApplicationLogRecord>> GetApplicationLogsAsync(ApplicationLogQuery query, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.GetApplicationLogsStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@TopN", SqlDbType.Int)
            {
                Value = query.TopN
            });
            command.Parameters.Add(new SqlParameter("@FromTimeStamp", SqlDbType.DateTime2)
            {
                Value = query.FromTimeStamp.HasValue ? query.FromTimeStamp.Value : DBNull.Value
            });
            command.Parameters.Add(new SqlParameter("@ToTimeStamp", SqlDbType.DateTime2)
            {
                Value = query.ToTimeStamp.HasValue ? query.ToTimeStamp.Value : DBNull.Value
            });
            command.Parameters.Add(new SqlParameter("@LevelsCsv", SqlDbType.NVarChar, 256)
            {
                Value = (object?)ApplicationLogFilterHelper.ToCsv(query.Levels) ?? DBNull.Value
            });
            command.Parameters.Add(new SqlParameter("@MessageContains", SqlDbType.NVarChar, 400)
            {
                Value = string.IsNullOrWhiteSpace(query.MessageContains) ? DBNull.Value : query.MessageContains.Trim()
            });

            await _connectionFactory.OpenAsync(connection, cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var idOrdinal = reader.GetOrdinal("Id");
            var timeStampOrdinal = reader.GetOrdinal("TimeStamp");
            var levelOrdinal = reader.GetOrdinal("Level");
            var messageOrdinal = reader.GetOrdinal("Message");
            var exceptionOrdinal = reader.GetOrdinal("Exception");
            var propertiesOrdinal = reader.GetOrdinal("Properties");

            var rows = new List<ApplicationLogRecord>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var timeStamp = SqlDataHelper.ReadNullableDateTime(reader, timeStampOrdinal)
                    ?? throw new InvalidOperationException("Application log row returned a null TimeStamp.");

                rows.Add(new ApplicationLogRecord(
                    reader.GetInt32(idOrdinal),
                    timeStamp,
                    SqlDataHelper.ReadNullableString(reader, levelOrdinal) ?? string.Empty,
                    SqlDataHelper.ReadNullableString(reader, messageOrdinal),
                    SqlDataHelper.ReadNullableString(reader, exceptionOrdinal),
                    SqlDataHelper.ReadNullableString(reader, propertiesOrdinal)));
            }

            return rows;
        }
        catch (SqlException ex)
        {
            LogSqlException(ex, query);
            throw;
        }
        catch (TimeoutException ex)
        {
            _logger.LogError(
                AppLogEvents.RepositoryApplicationLogsQueryTimeout,
                ex,
                "Application log query timed out for connection {ConnectionName}, command {CommandName}, timeout seconds {TimeoutSeconds}.",
                _options.ConnectionStringName,
                _options.GetApplicationLogsStoredProcedure,
                _options.CommandTimeoutSeconds);
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                AppLogEvents.RepositoryApplicationLogsQueryFailed,
                ex,
                "Application log query failed for command {CommandName}.",
                _options.GetApplicationLogsStoredProcedure);
            throw;
        }
        finally
        {
            LogOperationDuration(stopwatch.ElapsedMilliseconds);
        }
    }

    private void LogOperationDuration(long elapsedMilliseconds)
    {
        if (elapsedMilliseconds >= SlowOperationThresholdMilliseconds)
        {
            _logger.LogWarning(
                AppLogEvents.RepositoryApplicationLogsQuerySlow,
                "Application log query for command {CommandName} is slow. ElapsedMs={ElapsedMs}.",
                _options.GetApplicationLogsStoredProcedure,
                elapsedMilliseconds);
        }
        else
        {
            _logger.LogDebug(
                "Application log query for command {CommandName} completed in {ElapsedMs} ms.",
                _options.GetApplicationLogsStoredProcedure,
                elapsedMilliseconds);
        }
    }

    private void LogSqlException(SqlException ex, ApplicationLogQuery query)
    {
        if (SqlDataHelper.IsSqlTimeout(ex))
        {
            _logger.LogError(
                AppLogEvents.RepositoryApplicationLogsQueryTimeout,
                ex,
                "Application log SQL timeout for connection {ConnectionName}, command {CommandName}, timeout seconds {TimeoutSeconds}, SQL Number {SqlNumber}. TopN={TopN}, LevelCount={LevelCount}, HasMessageFilter={HasMessageFilter}.",
                _options.ConnectionStringName,
                _options.GetApplicationLogsStoredProcedure,
                _options.CommandTimeoutSeconds,
                ex.Number,
                query.TopN,
                query.Levels.Count,
                !string.IsNullOrWhiteSpace(query.MessageContains));
            return;
        }

        _logger.LogError(
            AppLogEvents.RepositoryApplicationLogsQueryFailed,
            ex,
            "Application log SQL error for connection {ConnectionName}, command {CommandName}, SQL Number {SqlNumber}, State {SqlState}, Class {SqlClass}. TopN={TopN}, LevelCount={LevelCount}, HasMessageFilter={HasMessageFilter}.",
            _options.ConnectionStringName,
            _options.GetApplicationLogsStoredProcedure,
            ex.Number,
            ex.State,
            ex.Class,
            query.TopN,
            query.Levels.Count,
            !string.IsNullOrWhiteSpace(query.MessageContains));
    }
}