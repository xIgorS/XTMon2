using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class BatchStatusRepository : IBatchStatusRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly BatchStatusOptions _options;
    private readonly ILogger<BatchStatusRepository> _logger;

    public BatchStatusRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<BatchStatusOptions> options,
        ILogger<BatchStatusRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<MonitoringTableResult> GetBatchStatusAsync(DateOnly pnlDate, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.CheckBatchStatusStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@pnldate", SqlDbType.Date)
            {
                Value = pnlDate.ToDateTime(TimeOnly.MinValue)
            });
            command.Parameters.Add(new SqlParameter("@sourcesytemcode", SqlDbType.NVarChar, 50)
            {
                Value = DBNull.Value
            });
            command.Parameters.Add(new SqlParameter("@IsStandalone", SqlDbType.Bit)
            {
                Value = true
            });
            command.Parameters.Add(new SqlParameter("@Execute", SqlDbType.Bit)
            {
                Value = true
            });
            command.Parameters.Add(new SqlParameter("@Query", SqlDbType.NVarChar, -1)
            {
                Direction = ParameterDirection.Output,
                Value = string.Empty
            });

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var table = await MonitoringRepository.ReadMonitoringTableAsync(reader, cancellationToken);

            while (await reader.NextResultAsync(cancellationToken))
            {
            }

            return table;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Batch status procedure execution failed for {StoredProcedure} and PnlDate {PnlDate}.",
                _options.CheckBatchStatusStoredProcedure,
                pnlDate);
            throw;
        }
    }
}