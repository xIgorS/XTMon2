using Microsoft.Data.SqlClient;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using System.Data;
using System.Globalization;
using System.Text.Json;
using System.Threading;

namespace XTMon.Data;

public sealed class StoredProcedureLogSink : ILogEventSink
{
    private readonly string _connectionString;
    private readonly string _storedProcedure;
    private readonly int _commandTimeoutSeconds;
    private int _fallbackFailureLogged;

    public StoredProcedureLogSink(string connectionString, string storedProcedure, int commandTimeoutSeconds)
    {
        _connectionString = connectionString;
        _storedProcedure = storedProcedure;
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    public void Emit(LogEvent logEvent)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            using var command = connection.CreateCommand();
            command.CommandText = _storedProcedure;
            command.CommandType = CommandType.StoredProcedure;
            command.CommandTimeout = _commandTimeoutSeconds;

            AddCommonParameters(command, logEvent);

            connection.Open();
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            TryWriteFallbackLog(logEvent, ex);
        }
    }

    private void TryWriteFallbackLog(LogEvent logEvent, Exception originalException)
    {
        try
        {
            Interlocked.Exchange(ref _fallbackFailureLogged, 0);

            var selfLogMessage = string.Format(
                CultureInfo.InvariantCulture,
                "StoredProcedureLogSink failed to write event via stored procedure {0}. Event timestamp: {1:o}, level: {2}, message: {3}. Exception: {4}",
                _storedProcedure,
                logEvent.Timestamp,
                logEvent.Level,
                logEvent.RenderMessage(CultureInfo.InvariantCulture),
                originalException);

            SelfLog.WriteLine(selfLogMessage);
        }
        catch (Exception selfLogException)
        {
            // Limit repeated sink-failure noise while still exposing root cause in SelfLog.
            if (Interlocked.Exchange(ref _fallbackFailureLogged, 1) == 0)
            {
                SelfLog.WriteLine(
                    "StoredProcedureLogSink failed. Stored procedure error: {0}. SelfLog write error: {1}",
                    originalException,
                    selfLogException);
            }
        }
    }

    private static void AddCommonParameters(SqlCommand command, LogEvent logEvent)
    {
        command.Parameters.Add(new SqlParameter("@TimeStamp", SqlDbType.DateTime2)
        {
            Value = logEvent.Timestamp.UtcDateTime
        });

        command.Parameters.Add(new SqlParameter("@Level", SqlDbType.NVarChar, 32)
        {
            Value = logEvent.Level.ToString()
        });

        command.Parameters.Add(CreateNVarCharMaxParameter("@Message", logEvent.RenderMessage(CultureInfo.InvariantCulture)));
        command.Parameters.Add(CreateNVarCharMaxParameter("@MessageTemplate", logEvent.MessageTemplate.Text));
        command.Parameters.Add(CreateNVarCharMaxParameter("@Exception", logEvent.Exception?.ToString()));
        command.Parameters.Add(CreateNVarCharMaxParameter("@Properties", SerializeProperties(logEvent) as string));
    }

    private static SqlParameter CreateNVarCharMaxParameter(string name, string? value)
    {
        return new SqlParameter(name, SqlDbType.NVarChar, -1)
        {
            Value = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value
        };
    }

    private static object SerializeProperties(LogEvent logEvent)
    {
        if (logEvent.Properties.Count == 0)
        {
            return DBNull.Value;
        }

        var dict = new Dictionary<string, string?>(logEvent.Properties.Count);
        foreach (var property in logEvent.Properties)
        {
            dict[property.Key] = property.Value.ToString();
        }

        return JsonSerializer.Serialize(dict);
    }
}