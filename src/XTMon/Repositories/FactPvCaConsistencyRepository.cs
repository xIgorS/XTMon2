using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class FactPvCaConsistencyRepository : IFactPvCaConsistencyRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly FactPvCaConsistencyOptions _options;
    private readonly ILogger<FactPvCaConsistencyRepository> _logger;

    public FactPvCaConsistencyRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<FactPvCaConsistencyOptions> options,
        ILogger<FactPvCaConsistencyRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<FactPvCaConsistencyResult> GetFactPvCaConsistencyAsync(DateOnly pnlDate, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.FactPvCaConsistencyStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.ToDateTime(TimeOnly.MinValue)
            });
            command.Parameters.Add(new SqlParameter("@Execute", SqlDbType.Bit)
            {
                Value = true
            });

            var queryParameter = new SqlParameter("@Query", SqlDbType.NVarChar, -1)
            {
                Direction = ParameterDirection.InputOutput,
                Value = string.Empty
            };
            command.Parameters.Add(queryParameter);

            await _connectionFactory.OpenAsync(connection, cancellationToken);

            MonitoringTableResult table;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                table = await MonitoringRepository.ReadMonitoringTableAsync(reader, cancellationToken);

                while (await reader.NextResultAsync(cancellationToken))
                {
                }
            }

            return new FactPvCaConsistencyResult(
                SqlDataHelper.ParseQuery(queryParameter.Value),
                table);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Fact PV/CA Consistency procedure execution failed for {StoredProcedure}, PnlDate {PnlDate}.",
                _options.FactPvCaConsistencyStoredProcedure,
                pnlDate);
            throw;
        }
    }
}