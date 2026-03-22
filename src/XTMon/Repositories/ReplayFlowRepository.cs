using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data;
using XTMon.Helpers;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Options;

namespace XTMon.Repositories;

public sealed class ReplayFlowRepository : IReplayFlowRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly ReplayFlowsOptions _replayOptions;
    private readonly ILogger<ReplayFlowRepository> _logger;

    public ReplayFlowRepository(
        SqlConnectionFactory connectionFactory,
        IOptions<ReplayFlowsOptions> replayOptions,
        ILogger<ReplayFlowRepository> logger)
    {
        _connectionFactory = connectionFactory;
        _replayOptions = replayOptions.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyList<FailedFlowRow>> GetFailedFlowsAsync(DateOnly? pnlDate, string? replayFlowSet, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_replayOptions.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _replayOptions.GetFailedFlowsStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _replayOptions.CommandTimeoutSeconds;

            var pnlDateParameter = new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.HasValue ? pnlDate.Value.ToDateTime(TimeOnly.MinValue) : DBNull.Value
            };
            command.Parameters.Add(pnlDateParameter);

            var replayFlowSetParameter = new SqlParameter("@ReplayFlowSet", SqlDbType.VarChar, 1000)
            {
                Value = string.IsNullOrWhiteSpace(replayFlowSet) ? DBNull.Value : replayFlowSet
            };
            command.Parameters.Add(replayFlowSetParameter);

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var flowIdIndex = reader.GetOrdinal("FlowId");
            var flowIdDerivedFromIndex = reader.GetOrdinal("FlowIdDerivedFrom");
            var businessDataTypeIndex = reader.GetOrdinal("BusinessDataType");
            var feedSourceIndex = reader.GetOrdinal("FeedSource");
            var pnlDateIndex = reader.GetOrdinal("PnlDate");
            var packageGuidIndex = reader.GetOrdinal("PackageGuid");
            var fileNameIndex = reader.GetOrdinal("FileName");
            var arrivalDateIndex = reader.GetOrdinal("ArrivalDate");
            var currentStepIndex = reader.GetOrdinal("CurrentStep");
            var isFailedIndex = reader.GetOrdinal("IsFailed");
            var typeOfCalculationIndex = reader.GetOrdinal("TypeOfCalculation");
            var isAdjustmentIndex = reader.GetOrdinal("IsAdjustment");
            var isReplayIndex = reader.GetOrdinal("IsReplay");
            var withBackdatedIndex = reader.GetOrdinal("WithBackdated");
            var skipCoreProcessIndex = reader.GetOrdinal("SkipCoreProcess");
            var droptabletmpIndex = reader.GetOrdinal("DropTableTmp");

            var rows = new List<FailedFlowRow>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var flowId = reader.IsDBNull(flowIdIndex) ? (long?)null : reader.GetInt64(flowIdIndex);
                var flowIdDerivedFrom = reader.IsDBNull(flowIdDerivedFromIndex) ? (long?)null : reader.GetInt64(flowIdDerivedFromIndex);
                var businessDataType = reader.IsDBNull(businessDataTypeIndex) ? null : reader.GetString(businessDataTypeIndex);
                var feedSource = reader.IsDBNull(feedSourceIndex) ? null : reader.GetString(feedSourceIndex);
                var pnlDateValue = DateOnly.FromDateTime(reader.GetDateTime(pnlDateIndex));
                var packageGuid = reader.GetGuid(packageGuidIndex);
                var fileName = reader.IsDBNull(fileNameIndex) ? null : reader.GetString(fileNameIndex);
                var arrivalDate = reader.IsDBNull(arrivalDateIndex) ? (DateTime?)null : reader.GetDateTime(arrivalDateIndex);
                var currentStep = reader.IsDBNull(currentStepIndex) ? null : reader.GetString(currentStepIndex);
                var isFailed = SqlDataHelper.ReadBoolean(reader, isFailedIndex);
                var typeOfCalculation = reader.IsDBNull(typeOfCalculationIndex) ? null : reader.GetString(typeOfCalculationIndex);
                var isAdjustment = SqlDataHelper.ReadBoolean(reader, isAdjustmentIndex);
                var isReplay = SqlDataHelper.ReadBoolean(reader, isReplayIndex);
                var withBackdated = SqlDataHelper.ReadBoolean(reader, withBackdatedIndex);
                var skipCoreProcess = SqlDataHelper.ReadBoolean(reader, skipCoreProcessIndex);
                var droptabletmp = SqlDataHelper.ReadBoolean(reader, droptabletmpIndex);

                rows.Add(new FailedFlowRow(
                    flowId,
                    flowIdDerivedFrom,
                    businessDataType,
                    feedSource,
                    pnlDateValue,
                    packageGuid,
                    fileName,
                    arrivalDate,
                    currentStep,
                    isFailed,
                    typeOfCalculation,
                    isAdjustment,
                    isReplay,
                    withBackdated,
                    skipCoreProcess,
                    droptabletmp));
            }

            return rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryFailedFlowsQueryFailed, ex, "Failed flows query failed for procedure {StoredProcedure}, PnlDate {PnlDate}, ReplayFlowSet {ReplayFlowSet}.", _replayOptions.GetFailedFlowsStoredProcedure, pnlDate, replayFlowSet);
            throw;
        }
    }

    public async Task<IReadOnlyList<ReplayFlowResultRow>> ReplayFlowsAsync(IReadOnlyCollection<ReplayFlowSubmissionRow> rows, string userId, CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            _logger.LogWarning(AppLogEvents.RepositoryReplaySubmitEmptyRows, "Replay flow submission was requested with zero rows for user {UserId}.", userId);
            return Array.Empty<ReplayFlowResultRow>();
        }

        try
        {
            using var connection = _connectionFactory.CreateConnection(_replayOptions.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _replayOptions.ReplayFlowsStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _replayOptions.CommandTimeoutSeconds;

            var table = new DataTable();
            table.Columns.Add("FlowIdDerivedFrom", typeof(long));
            table.Columns.Add("FlowId", typeof(long));
            table.Columns.Add("PnlDate", typeof(DateTime));
            table.Columns.Add("PackageGuid", typeof(Guid));
            table.Columns.Add("WithBackdated", typeof(bool));
            table.Columns.Add("SkipCoreProcess", typeof(bool));
            table.Columns.Add("DropTableTmp", typeof(bool));

            foreach (var row in rows)
            {
                table.Rows.Add(
                    row.FlowIdDerivedFrom,
                    row.FlowId,
                    row.PnlDate.ToDateTime(TimeOnly.MinValue),
                    row.PackageGuid,
                    row.WithBackdated,
                    row.SkipCoreProcess,
                    row.DropTableTmp);
            }

            var userIdParameter = new SqlParameter("@UserId", SqlDbType.VarChar, 100)
            {
                Value = userId
            };
            command.Parameters.Add(userIdParameter);

            var replayFlowsTableTypeName = string.IsNullOrWhiteSpace(_replayOptions.ReplayFlowsTableTypeName)
                ? "Replay.ReplayAdjAtCoreSet"
                : _replayOptions.ReplayFlowsTableTypeName;

            var parameter = new SqlParameter("@FlowData", SqlDbType.Structured)
            {
                TypeName = replayFlowsTableTypeName,
                Value = table
            };
            command.Parameters.Add(parameter);

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                ordinals[reader.GetName(i)] = i;
            }

            var requiredColumns = new[]
            {
                "FlowId",
                "FlowIdDerivedFrom",
                "PnlDate",
                "PackageGuid",
                "WithBackdated",
                "SkipCoreProcess",
                "DropTableTmp",
                "DateCreated",
                "CreatedBy"
            };

            var missingColumns = requiredColumns.Where(column => !ordinals.ContainsKey(column)).ToList();
            if (missingColumns.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Replay flows result is missing column(s): {string.Join(", ", missingColumns)}.");
            }

            var flowIdIndex = ordinals["FlowId"];
            var flowIdDerivedFromIndex = ordinals["FlowIdDerivedFrom"];
            var pnlDateIndex = ordinals["PnlDate"];
            var packageGuidIndex = ordinals["PackageGuid"];
            var withBackdatedIndex = ordinals["WithBackdated"];
            var skipCoreProcessIndex = ordinals["SkipCoreProcess"];
            var droptabletmpIndex = ordinals["DropTableTmp"];
            var dateCreatedIndex = ordinals["DateCreated"];
            var createdByIndex = ordinals["CreatedBy"];

            var results = new List<ReplayFlowResultRow>();
            while (await reader.ReadAsync(cancellationToken))
            {
                results.Add(new ReplayFlowResultRow(
                    reader.GetInt64(flowIdDerivedFromIndex),
                    reader.GetInt64(flowIdIndex),
                    DateOnly.FromDateTime(reader.GetDateTime(pnlDateIndex)),
                    reader.GetGuid(packageGuidIndex),
                    SqlDataHelper.ReadBoolean(reader, withBackdatedIndex),
                    SqlDataHelper.ReadBoolean(reader, skipCoreProcessIndex),
                    SqlDataHelper.ReadBoolean(reader, droptabletmpIndex),
                    reader.GetDateTime(dateCreatedIndex),
                    reader.GetString(createdByIndex)));
            }

            return results;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryReplaySubmitFailed, ex, "Replay flow submit failed for {RowCount} row(s), procedure {StoredProcedure}, user {UserId}.", rows.Count, _replayOptions.ReplayFlowsStoredProcedure, userId);
            throw;
        }
    }

    public async Task ProcessReplayFlowsAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_replayOptions.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _replayOptions.ProcessReplayFlowsStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _replayOptions.ProcessCommandTimeoutSeconds;

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryReplayProcessFailed, ex, "Replay flow processing failed for procedure {StoredProcedure}.", _replayOptions.ProcessReplayFlowsStoredProcedure);
            throw;
        }
    }

    public async Task<IReadOnlyList<ReplayFlowStatusRow>> GetReplayFlowStatusAsync(DateOnly? pnlDate, CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_replayOptions.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _replayOptions.GetReplayFlowStatusStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _replayOptions.CommandTimeoutSeconds;

            var pnlDateParameter = new SqlParameter("@PnlDate", SqlDbType.Date)
            {
                Value = pnlDate.HasValue ? pnlDate.Value.ToDateTime(TimeOnly.MinValue) : DBNull.Value
            };
            command.Parameters.Add(pnlDateParameter);

            await connection.OpenAsync(cancellationToken);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                ordinals[reader.GetName(i)] = i;
            }

            var requiredColumns = new[]
            {
                "FlowId",
                "FlowIdDerivedFrom",
                "PnlDate",
                "PackageGuid",
                "WithBackdated",
                "SkipCoreProcess",
                "DropTableTmp",
                "DateCreated",
                "CreatedBy",
                "DateStarted",
                "DateCompleted"
            };

            var missingColumns = requiredColumns.Where(column => !ordinals.ContainsKey(column)).ToList();
            if (missingColumns.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Replay flow status result is missing column(s): {string.Join(", ", missingColumns)}.");
            }

            var flowIdIndex = ordinals["FlowId"];
            var flowIdDerivedFromIndex = ordinals["FlowIdDerivedFrom"];
            var pnlDateIndex = ordinals["PnlDate"];
            var packageGuidIndex = ordinals["PackageGuid"];
            var withBackdatedIndex = ordinals["WithBackdated"];
            var skipCoreProcessIndex = ordinals["SkipCoreProcess"];
            var droptabletmpIndex = ordinals["DropTableTmp"];
            var dateCreatedIndex = ordinals["DateCreated"];
            var createdByIndex = ordinals["CreatedBy"];
            var dateSubmittedIndex = ordinals.TryGetValue("DateSubmitted", out var dateSubmittedOrdinal) ? dateSubmittedOrdinal : (int?)null;
            var dateStartedIndex = ordinals["DateStarted"];
            var dateCompletedIndex = ordinals["DateCompleted"];
            var statusIndex = ordinals.TryGetValue("ReplayStatus", out var statusOrdinal) ? statusOrdinal : (int?)null;
            var processStatusIndex = ordinals.TryGetValue("ProcessStatus", out var processStatusOrdinal) ? processStatusOrdinal : (int?)null;
            var durationIndex = ordinals.TryGetValue("Duration", out var durationOrdinal) ? durationOrdinal : (int?)null;

            var results = new List<ReplayFlowStatusRow>();
            while (await reader.ReadAsync(cancellationToken))
            {
                results.Add(new ReplayFlowStatusRow(
                    reader.GetInt64(flowIdIndex),
                    reader.GetInt64(flowIdDerivedFromIndex),
                    DateOnly.FromDateTime(reader.GetDateTime(pnlDateIndex)),
                    reader.GetGuid(packageGuidIndex),
                    SqlDataHelper.ReadBoolean(reader, withBackdatedIndex),
                    SqlDataHelper.ReadBoolean(reader, skipCoreProcessIndex),
                    SqlDataHelper.ReadBoolean(reader, droptabletmpIndex),
                    reader.GetDateTime(dateCreatedIndex),
                    reader.GetString(createdByIndex),
                    dateSubmittedIndex.HasValue && !reader.IsDBNull(dateSubmittedIndex.Value) ? reader.GetDateTime(dateSubmittedIndex.Value) : null,
                    reader.IsDBNull(dateStartedIndex) ? null : reader.GetDateTime(dateStartedIndex),
                    reader.IsDBNull(dateCompletedIndex) ? null : reader.GetDateTime(dateCompletedIndex),
                    SqlDataHelper.ReadNullableString(reader, statusIndex),
                    SqlDataHelper.ReadNullableString(reader, processStatusIndex),
                    SqlDataHelper.ReadNullableString(reader, durationIndex)));
            }

            return results;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryReplayStatusFailed, ex, "Replay flow status query failed for procedure {StoredProcedure} and PnlDate {PnlDate}.", _replayOptions.GetReplayFlowStatusStoredProcedure, pnlDate);
            throw;
        }
    }

    public async Task RefreshReplayFlowProcessStatusAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection(_replayOptions.ConnectionStringName);
            using var command = connection.CreateCommand();
            command.CommandText = _replayOptions.GetReplayFlowProcessStatusStoredProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _replayOptions.CommandTimeoutSeconds;

            await connection.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(AppLogEvents.RepositoryReplayStatusFailed, ex, "Replay flow process status refresh failed for procedure {StoredProcedure}.", _replayOptions.GetReplayFlowProcessStatusStoredProcedure);
            throw;
        }
    }
}
