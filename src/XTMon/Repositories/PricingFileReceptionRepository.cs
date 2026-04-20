using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class PricingFileReceptionRepository : IPricingFileReceptionRepository
{
    private const string LastVersionParameterName = "@LastVersion";

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly PricingFileReceptionOptions _options;
    private readonly ILogger<PricingFileReceptionRepository> _logger;

    public PricingFileReceptionRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<PricingFileReceptionOptions> options,
        ILogger<PricingFileReceptionRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<PricingFileReceptionResult> GetPricingFileReceptionAsync(DateOnly pnlDate, bool traceAllVersions, CancellationToken cancellationToken)
    {
        try
        {
            return await ExecutePricingFileReceptionAsync(pnlDate, traceAllVersions, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Pricing File Reception procedure execution failed for {StoredProcedure}, PnlDate {PnlDate}, TraceAllVersions {TraceAllVersions}.",
                _options.PricingFileReceptionStoredProcedure,
                pnlDate,
                traceAllVersions);
            throw;
        }
    }

    private async Task<PricingFileReceptionResult> ExecutePricingFileReceptionAsync(
        DateOnly pnlDate,
        bool traceAllVersions,
        CancellationToken cancellationToken)
    {
        using var connection = _connectionFactory.CreateConnection(_options.ConnectionStringName);
        using var command = connection.CreateCommand();
        command.CommandText = _options.PricingFileReceptionStoredProcedure;
        command.CommandType = CommandType.StoredProcedure;
        command.CommandTimeout = _options.CommandTimeoutSeconds;

        command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date)
        {
            Value = pnlDate.ToDateTime(TimeOnly.MinValue)
        });
        command.Parameters.Add(new SqlParameter("@SourceSystemCodes", SqlDbType.VarChar, 4000)
        {
            Value = DBNull.Value
        });
        command.Parameters.Add(new SqlParameter(LastVersionParameterName, SqlDbType.Bit)
        {
            Value = !traceAllVersions
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

        return new PricingFileReceptionResult(
            SqlDataHelper.ParseQuery(queryParameter.Value),
            table);
    }
}
