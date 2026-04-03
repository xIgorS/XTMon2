using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class ReferentialDataRepository : IReferentialDataRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly ReferentialDataOptions _options;
    private readonly ILogger<ReferentialDataRepository> _logger;

    public ReferentialDataRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<ReferentialDataOptions> options,
        ILogger<ReferentialDataRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<ReferentialDataResult> GetReferentialDataAsync(DateOnly pnlDate, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.CheckReferentialDataStoredProcedure;
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
                Direction = ParameterDirection.Output,
                Value = string.Empty
            };
            command.Parameters.Add(queryParameter);

            await connection.OpenAsync(cancellationToken);

            MonitoringTableResult table;
            string? queryFromResults = null;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                table = await MonitoringRepository.ReadMonitoringTableAsync(reader, cancellationToken);

                while (await reader.NextResultAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                continue;
                            }

                            var text = Convert.ToString(reader.GetValue(i));
                            if (!string.IsNullOrWhiteSpace(text))
                            {
                                queryFromResults = text;
                                break;
                            }
                        }

                        if (!string.IsNullOrWhiteSpace(queryFromResults))
                        {
                            break;
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(queryFromResults))
                    {
                        break;
                    }
                }
            }

            var parsedQuery = SqlDataHelper.ParseQuery(queryParameter.Value);
            if (string.IsNullOrWhiteSpace(parsedQuery))
            {
                parsedQuery = SqlDataHelper.ParseQuery(queryFromResults);
            }

            return new ReferentialDataResult(
                parsedQuery,
                table);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Referential data procedure execution failed for {StoredProcedure} and PnlDate {PnlDate}.",
                _options.CheckReferentialDataStoredProcedure,
                pnlDate);
            throw;
        }
    }
}