using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class PricingRepository : IPricingRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly PricingOptions _options;
    private readonly ILogger<PricingRepository> _logger;

    public PricingRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<PricingOptions> options,
        ILogger<PricingRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<PricingSourceSystem>> GetSourceSystemsAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.GetAllSourceSystemsStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var sourceSystemCodeOrdinal = SqlDataHelper.FindOrdinal(reader, "SourceSystemCode");
            var sourceSystems = new List<PricingSourceSystem>();
            var seenCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            while (await reader.ReadAsync(cancellationToken))
            {
                var sourceSystemCode = ReadSourceSystemCode(reader, sourceSystemCodeOrdinal);
                if (string.IsNullOrWhiteSpace(sourceSystemCode) || !seenCodes.Add(sourceSystemCode))
                {
                    continue;
                }

                sourceSystems.Add(new PricingSourceSystem(sourceSystemCode));
            }

            return sourceSystems;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Pricing source-system procedure execution failed for {StoredProcedure}.",
                _options.GetAllSourceSystemsStoredProcedure);
            throw;
        }
    }

    public async Task<PricingResult> GetPricingAsync(DateOnly pnlDate, string? sourceSystemCodes, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.PricingStoredProcedure;
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

            await connection.OpenAsync(cancellationToken);

            MonitoringTableResult table;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                table = await MonitoringRepository.ReadMonitoringTableAsync(reader, cancellationToken);

                while (await reader.NextResultAsync(cancellationToken))
                {
                }
            }

            return new PricingResult(
                SqlDataHelper.ParseQuery(queryParameter.Value),
                table);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Pricing procedure execution failed for {StoredProcedure}, PnlDate {PnlDate}, SourceSystemCodes {SourceSystemCodes}.",
                _options.PricingStoredProcedure,
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
}