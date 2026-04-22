using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class FunctionalRejectionRepository : IFunctionalRejectionRepository
{
    private static readonly string[] DisplayCodeAliases =
    [
        "sourcesystembusinessdatatypecode",
        "SourceSystemBusinessDataTypeCode",
        "SourceSystemBusinessDatatypeCode"
    ];

    private static readonly string[] BusinessDataTypeIdAliases =
    [
        "businessdatatypeid",
        "BusinessDataTypeId",
        "BusinessDatatypeId",
        "SourceSystemBusinessDataTypeId",
        "SourceSystemBusinessDatatypeId"
    ];

    private static readonly string[] SourceSystemNameAliases =
    [
        "sourcesystemname",
        "SourceSystemName",
        "SourcesystemName",
        "sourcesystemcode",
        "SourceSystemCode",
        "SourcesystemCode"
    ];

    private static readonly string[] DbConnectionAliases =
    [
        "dbconnexion",
        "DbConnexion",
        "DbConnection",
        "dbconnection"
    ];

    private static readonly string[] MetadataNameAliases =
    [
        "ColumnName",
        "Name",
        "FieldName",
        "Column",
        "Column_Name"
    ];

    private static readonly string[] MetadataTypeAliases =
    [
        "DataType",
        "Type",
        "SqlType",
        "TypeName",
        "ColumnType"
    ];

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly FunctionalRejectionOptions _options;
    private readonly ILogger<FunctionalRejectionRepository> _logger;

    public FunctionalRejectionRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<FunctionalRejectionOptions> options,
        ILogger<FunctionalRejectionRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<FunctionalRejectionMenuItem>> GetMenuItemsAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_options.MenuConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _options.SourceSystemTechnicalRejectStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (reader.FieldCount <= 0)
            {
                return Array.Empty<FunctionalRejectionMenuItem>();
            }

            var codeOrdinal = ResolveRequiredOrdinal(
                reader,
                DisplayCodeAliases,
                static name => name.Contains("businessdatatypecode", StringComparison.OrdinalIgnoreCase));
            var businessDataTypeIdOrdinal = ResolveRequiredOrdinal(
                reader,
                BusinessDataTypeIdAliases,
                static name => name.Contains("datatypeid", StringComparison.OrdinalIgnoreCase));
            var sourceSystemNameOrdinal = ResolveRequiredOrdinal(
                reader,
                SourceSystemNameAliases,
                static name => name.Contains("sourcesystem", StringComparison.OrdinalIgnoreCase) &&
                               (name.Contains("name", StringComparison.OrdinalIgnoreCase) ||
                                name.Contains("code", StringComparison.OrdinalIgnoreCase)));
            var dbConnectionOrdinal = ResolveRequiredOrdinal(
                reader,
                DbConnectionAliases,
                static name => name.Contains("db", StringComparison.OrdinalIgnoreCase) &&
                               (name.Contains("connexion", StringComparison.OrdinalIgnoreCase) ||
                                name.Contains("connection", StringComparison.OrdinalIgnoreCase)));

            var items = new List<FunctionalRejectionMenuItem>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            while (await reader.ReadAsync(cancellationToken))
            {
                var code = SqlDataHelper.ReadNullableString(reader, codeOrdinal)?.Trim();
                var sourceSystemName = SqlDataHelper.ReadNullableString(reader, sourceSystemNameOrdinal)?.Trim();
                var dbConnection = SqlDataHelper.ReadNullableString(reader, dbConnectionOrdinal)?.Trim();
                if (string.IsNullOrWhiteSpace(code) || string.IsNullOrWhiteSpace(sourceSystemName) || string.IsNullOrWhiteSpace(dbConnection))
                {
                    continue;
                }

                if (!TryReadInt32(reader, businessDataTypeIdOrdinal, out var businessDataTypeId))
                {
                    continue;
                }

                var dedupeKey = $"{code}|{businessDataTypeId}|{sourceSystemName}|{dbConnection}";
                if (!seen.Add(dedupeKey))
                {
                    continue;
                }

                items.Add(new FunctionalRejectionMenuItem(code, businessDataTypeId, sourceSystemName, dbConnection));
            }

            return items
                .OrderBy(static item => item.SourceSystemBusinessDataTypeCode, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Functional Rejection menu procedure execution failed for {StoredProcedure}.",
                _options.SourceSystemTechnicalRejectStoredProcedure);
            throw;
        }
    }

    public async Task<TechnicalRejectResult> GetTechnicalRejectAsync(
        DateOnly pnlDate,
        int businessDataTypeId,
        string dbConnection,
        string sourceSystemName,
        CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(ResolveDetailConnectionStringName(dbConnection));
            using var command = connection.CreateCommand();
            command.CommandText = _options.TechnicalRejectStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _options.CommandTimeoutSeconds;

            command.Parameters.Add(new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.ToDateTime(TimeOnly.MinValue)
            });
            command.Parameters.Add(new SqlParameter("@businessdatatypeid", SqlDbType.Int)
            {
                Value = businessDataTypeId
            });
            command.Parameters.Add(new SqlParameter("@SourcesystemName", SqlDbType.VarChar, 50)
            {
                Value = sourceSystemName
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

            var columns = Array.Empty<TechnicalRejectColumn>();
            MonitoringTableResult table = new(Array.Empty<string>(), Array.Empty<IReadOnlyList<string?>>());

            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                columns = await ReadTechnicalRejectColumnsAsync(reader, cancellationToken);

                if (await reader.NextResultAsync(cancellationToken))
                {
                    table = await MonitoringRepository.ReadMonitoringTableAsync(reader, cancellationToken);
                }

                while (await reader.NextResultAsync(cancellationToken))
                {
                }
            }

            var alignedColumns = AlignColumns(columns, table.Columns);
            return new TechnicalRejectResult(
                SqlDataHelper.ParseQuery(queryParameter.Value),
                alignedColumns,
                table.Rows);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                AppLogEvents.RepositoryMonitoringProcedureFailed,
                ex,
                "Functional Rejection procedure execution failed for {StoredProcedure}, DbConnection {DbConnection}, PnlDate {PnlDate}, BusinessDataTypeId {BusinessDataTypeId}, SourceSystemName {SourceSystemName}.",
                _options.TechnicalRejectStoredProcedure,
                dbConnection,
                pnlDate,
                businessDataTypeId,
                sourceSystemName);
            throw;
        }
    }

    private string ResolveDetailConnectionStringName(string dbConnection)
        => _options.ResolveDetailConnectionStringName(dbConnection);

    private static async Task<TechnicalRejectColumn[]> ReadTechnicalRejectColumnsAsync(SqlDataReader reader, CancellationToken cancellationToken)
    {
        if (reader.FieldCount <= 0)
        {
            return Array.Empty<TechnicalRejectColumn>();
        }

        var nameOrdinal = ResolveOptionalOrdinal(
            reader,
            MetadataNameAliases,
            static name => name.Contains("column", StringComparison.OrdinalIgnoreCase) &&
                           name.Contains("name", StringComparison.OrdinalIgnoreCase));
        var typeOrdinal = ResolveOptionalOrdinal(
            reader,
            MetadataTypeAliases,
            static name => name.Contains("type", StringComparison.OrdinalIgnoreCase));

        var columns = new List<TechnicalRejectColumn>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var name = ReadMetadataValue(reader, nameOrdinal, 0);
            var typeName = ReadMetadataValue(reader, typeOrdinal, 1);
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            columns.Add(new TechnicalRejectColumn(name.Trim(), typeName?.Trim() ?? string.Empty));
        }

        return columns.ToArray();
    }

    private static IReadOnlyList<TechnicalRejectColumn> AlignColumns(
        IReadOnlyList<TechnicalRejectColumn> metadataColumns,
        IReadOnlyList<string> dataColumns)
    {
        if (dataColumns.Count == 0)
        {
            return metadataColumns;
        }

        var metadataByName = new Dictionary<string, TechnicalRejectColumn>(StringComparer.OrdinalIgnoreCase);
        foreach (var column in metadataColumns)
        {
            var normalizedName = Normalize(column.Name);
            if (!string.IsNullOrWhiteSpace(normalizedName))
            {
                metadataByName[normalizedName] = column;
            }
        }

        var alignedColumns = new List<TechnicalRejectColumn>(dataColumns.Count);
        for (var i = 0; i < dataColumns.Count; i++)
        {
            var dataColumnName = dataColumns[i];
            if (metadataByName.TryGetValue(Normalize(dataColumnName), out var metadataColumn))
            {
                alignedColumns.Add(new TechnicalRejectColumn(
                    string.IsNullOrWhiteSpace(metadataColumn.Name) ? dataColumnName : metadataColumn.Name,
                    metadataColumn.TypeName));
                continue;
            }

            if (i < metadataColumns.Count)
            {
                alignedColumns.Add(new TechnicalRejectColumn(dataColumnName, metadataColumns[i].TypeName));
                continue;
            }

            alignedColumns.Add(new TechnicalRejectColumn(dataColumnName, string.Empty));
        }

        return alignedColumns;
    }

    private static string? ReadMetadataValue(SqlDataReader reader, int? preferredOrdinal, int fallbackOrdinal)
    {
        var value = SqlDataHelper.ReadNullableString(reader, preferredOrdinal)?.Trim();
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        if (fallbackOrdinal >= 0 && fallbackOrdinal < reader.FieldCount)
        {
            return SqlDataHelper.ReadNullableString(reader, fallbackOrdinal)?.Trim();
        }

        return null;
    }

    private static bool TryReadInt32(SqlDataReader reader, int ordinal, out int value)
    {
        value = default;
        if (reader.IsDBNull(ordinal))
        {
            return false;
        }

        var rawValue = reader.GetValue(ordinal);
        switch (rawValue)
        {
            case int intValue:
                value = intValue;
                return true;
            case short shortValue:
                value = shortValue;
                return true;
            case long longValue when longValue is >= int.MinValue and <= int.MaxValue:
                value = (int)longValue;
                return true;
            default:
                return int.TryParse(Convert.ToString(rawValue), out value);
        }
    }

    private static int ResolveRequiredOrdinal(
        IDataRecord reader,
        IReadOnlyList<string> aliases,
        Func<string, bool> fallbackMatch)
    {
        var ordinal = ResolveOptionalOrdinal(reader, aliases, fallbackMatch);
        if (ordinal.HasValue)
        {
            return ordinal.Value;
        }

        throw new InvalidOperationException($"Unable to locate required Functional Rejection column. Available columns: {string.Join(", ", GetColumnNames(reader))}");
    }

    private static int? ResolveOptionalOrdinal(
        IDataRecord reader,
        IReadOnlyList<string> aliases,
        Func<string, bool> fallbackMatch)
    {
        foreach (var alias in aliases)
        {
            var aliasOrdinal = SqlDataHelper.FindOrdinal(reader, alias);
            if (aliasOrdinal.HasValue)
            {
                return aliasOrdinal.Value;
            }
        }

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var columnName = reader.GetName(i);
            if (fallbackMatch(columnName))
            {
                return i;
            }
        }

        return null;
    }

    private static IEnumerable<string> GetColumnNames(IDataRecord reader)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            yield return reader.GetName(i);
        }
    }

    private static string Normalize(string value)
    {
        return new string(value
            .Where(static ch => char.IsLetterOrDigit(ch))
            .Select(static ch => char.ToLowerInvariant(ch))
            .ToArray());
    }
}