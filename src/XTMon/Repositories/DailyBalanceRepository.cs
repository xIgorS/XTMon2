using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;
using System.Globalization;

namespace XTMon.Repositories;

public sealed class DailyBalanceRepository : IDailyBalanceRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly DailyBalanceOptions _options;
    private readonly ILogger<DailyBalanceRepository> _logger;

    public DailyBalanceRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<DailyBalanceOptions> options,
        ILogger<DailyBalanceRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<DailyBalanceSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.GetAllSourceSystemsStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            await _connectionFactory.OpenAsync(connection, cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var sourceSystemCodeOrdinal = SqlDataHelper.FindOrdinal(reader, "SourceSystemCode");
            var sourceSystems = new List<DailyBalanceSourceSystem>();
            var seenCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            while (await reader.ReadAsync(cancellationToken))
            {
                var sourceSystemCode = ReadSourceSystemCode(reader, sourceSystemCodeOrdinal);
                if (string.IsNullOrWhiteSpace(sourceSystemCode) || !seenCodes.Add(sourceSystemCode))
                {
                    continue;
                }

                sourceSystems.Add(new DailyBalanceSourceSystem(sourceSystemCode));
            }

            return sourceSystems;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Daily Balance source-system procedure execution failed for {StoredProcedure}.",
                _options.GetAllSourceSystemsStoredProcedure);
            throw;
        }
    }

    public async Task<DailyBalanceResult> GetDailyBalanceAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.DailyBalanceStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.ToDateTime(TimeOnly.MinValue)
            });
            command.Parameters.Add(new SqlParameter("@SourceSystemCodes", SqlDbType.VarChar, 4000)
            {
                Value = string.IsNullOrWhiteSpace(sourceSystemCodes) ? DBNull.Value : sourceSystemCodes
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
                table = await ReadDailyBalanceTableAsync(reader, cancellationToken);

                while (await reader.NextResultAsync(cancellationToken))
                {
                }
            }

            return new DailyBalanceResult(
                SqlDataHelper.ParseQuery(queryParameter.Value),
                table);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Daily Balance procedure execution failed for {StoredProcedure}, PnlDate {PnlDate}, SourceSystemCodes {SourceSystemCodes}.",
                _options.DailyBalanceStoredProcedure,
                pnlDate,
                sourceSystemCodes);
            throw;
        }
    }

    private static string? ReadSourceSystemCode(SqlDataReader reader, int? sourceSystemCodeOrdinal)
    {
        var sourceSystemCode = SqlDataHelper.ReadNullableString(reader, sourceSystemCodeOrdinal);
        if (!string.IsNullOrWhiteSpace(sourceSystemCode))
        {
            return sourceSystemCode.Trim();
        }

        for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
        {
            var fallbackValue = SqlDataHelper.ReadNullableString(reader, ordinal);
            if (!string.IsNullOrWhiteSpace(fallbackValue))
            {
                return fallbackValue.Trim();
            }
        }

        return null;
    }

    private static async Task<MonitoringTableResult> ReadDailyBalanceTableAsync(SqlDataReader reader, CancellationToken cancellationToken)
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
                    DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    decimal dec => dec.ToString("0.0000000000", CultureInfo.InvariantCulture),
                    double dbl => dbl.ToString("0.0000000000", CultureInfo.InvariantCulture),
                    float flt => flt.ToString("0.0000000000", CultureInfo.InvariantCulture),
                    byte number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    sbyte number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    short number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    ushort number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    int number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    uint number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    long number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    ulong number => ((decimal)number).ToString("0.0000000000", CultureInfo.InvariantCulture),
                    _ => value.ToString()
                };
            }

            rows.Add(row);
        }

        return new MonitoringTableResult(columns, rows);
    }
}
