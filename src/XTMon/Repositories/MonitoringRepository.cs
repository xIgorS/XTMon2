using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class MonitoringRepository : IMonitoringRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly MonitoringOptions _options;
    private readonly ILogger<MonitoringRepository> _logger;

    public MonitoringRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<MonitoringOptions> options,
        ILogger<MonitoringRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<MonitoringTableResult> GetDbSizePlusDiskAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await ExecuteMonitoringTableProcedureAsync(_options.DbSizePlusDiskStoredProcedure, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring procedure execution failed for {StoredProcedure}.", _options.DbSizePlusDiskStoredProcedure);
            throw;
        }
    }

    public async Task<MonitoringTableResult> GetDbBackupsAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await ExecuteMonitoringTableProcedureAsync(_options.DbBackupsStoredProcedure, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryMonitoringProcedureFailed, ex, "Monitoring procedure execution failed for {StoredProcedure}.", _options.DbBackupsStoredProcedure);
            throw;
        }
    }

    private async Task<MonitoringTableResult> ExecuteMonitoringTableProcedureAsync(string storedProcedure, CancellationToken cancellationToken)
    {
        using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
        using var command = connection.CreateCommand();
        command.CommandText = storedProcedure;
        command.CommandType = CommandType.StoredProcedure;
        command.CommandTimeout = _options.CommandTimeoutSeconds;

        await _connectionFactory.OpenAsync(connection, cancellationToken);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        return await ReadMonitoringTableAsync(reader, cancellationToken);
    }

    internal static async Task<MonitoringTableResult> ReadMonitoringTableAsync(SqlDataReader reader, CancellationToken cancellationToken)
    {
        if (reader.FieldCount <= 0)
        {
            return new MonitoringTableResult(Array.Empty<string>(), Array.Empty<IReadOnlyList<string?>>());
        }

        var columns = new List<string>();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            columns.Add(reader.GetName(i));
        }

        var rows = new List<IReadOnlyList<string?>>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new string?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (reader.IsDBNull(i))
                {
                    row[i] = null;
                    continue;
                }

                var value = reader.GetValue(i);
                row[i] = value switch
                {
                    DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss"),
                    DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss"),
                    decimal dec => dec.ToString("0.##"),
                    double dbl => dbl.ToString("0.##"),
                    float flt => flt.ToString("0.##"),
                    _ => value.ToString()
                };
            }

            rows.Add(row);
        }

        return new MonitoringTableResult(columns, rows);
    }
}
