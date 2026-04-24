using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class SystemDiagnosticsRepository : ISystemDiagnosticsRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly SystemDiagnosticsOptions _options;
    private readonly ILogger<SystemDiagnosticsRepository> _logger;

    public SystemDiagnosticsRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<SystemDiagnosticsOptions> options,
        ILogger<SystemDiagnosticsRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<int> CleanLoggingAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.CleanLoggingStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            var deletedRowsParameter = new SqlParameter("@DeletedRows", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            command.Parameters.Add(deletedRowsParameter);

            await _connectionFactory.OpenAsync(connection, cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);

            return deletedRowsParameter.Value is int deletedRows
                ? deletedRows
                : Convert.ToInt32(deletedRowsParameter.Value, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.DiagnosticsCheckFailed, ex, "System diagnostics logging cleanup failed for {StoredProcedure}.", _options.CleanLoggingStoredProcedure);
            throw;
        }
    }

    public async Task<SystemDiagnosticsHistoryCleanupResult> CleanHistoryAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.CleanHistoryStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            var monitoringLatestResultsDeletedParameter = CreateOutputParameter("@MonitoringLatestResultsDeleted");
            var monitoringJobsDeletedParameter = CreateOutputParameter("@MonitoringJobsDeleted");
            var jvCalculationJobResultsDeletedParameter = CreateOutputParameter("@JvCalculationJobResultsDeleted");
            var jvCalculationJobsDeletedParameter = CreateOutputParameter("@JvCalculationJobsDeleted");

            command.Parameters.Add(monitoringLatestResultsDeletedParameter);
            command.Parameters.Add(monitoringJobsDeletedParameter);
            command.Parameters.Add(jvCalculationJobResultsDeletedParameter);
            command.Parameters.Add(jvCalculationJobsDeletedParameter);

            await _connectionFactory.OpenAsync(connection, cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);

            return new SystemDiagnosticsHistoryCleanupResult(
                GetOutputInt(monitoringLatestResultsDeletedParameter),
                GetOutputInt(monitoringJobsDeletedParameter),
                GetOutputInt(jvCalculationJobResultsDeletedParameter),
                GetOutputInt(jvCalculationJobsDeletedParameter));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.DiagnosticsCheckFailed, ex, "System diagnostics history cleanup failed for {StoredProcedure}.", _options.CleanHistoryStoredProcedure);
            throw;
        }
    }

    private static SqlParameter CreateOutputParameter(string name)
    {
        return new SqlParameter(name, SqlDbType.Int)
        {
            Direction = ParameterDirection.Output
        };
    }

    private static int GetOutputInt(SqlParameter parameter)
    {
        return parameter.Value is int value
            ? value
            : Convert.ToInt32(parameter.Value, System.Globalization.CultureInfo.InvariantCulture);
    }
}