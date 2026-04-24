using Microsoft.Data.SqlClient;

namespace XTMon.Helpers;

internal static class SystemDiagnosticsErrorHelper
{
    public static string BuildFailureMessage(Exception exception, string fallbackMessage)
    {
        if (TryBuildSqlFailureMessage(exception, out var message))
        {
            return message;
        }

        return fallbackMessage;
    }

    private static bool TryBuildSqlFailureMessage(Exception exception, out string message)
    {
        if (exception is SqlException sqlException)
        {
            var normalizedMessage = NormalizeMessage(sqlException.Message);

            if (SqlDataHelper.IsMissingStoredProcedure(sqlException))
            {
                var procedureName = TryExtractStoredProcedureName(normalizedMessage);
                message = procedureName is null
                    ? "A required stored procedure is missing in the target database."
                    : $"Stored procedure {procedureName} is missing in the target database.";
                return true;
            }

            if (SqlDataHelper.IsSqlConnectionFailure(sqlException))
            {
                message = $"Database connection failed: {normalizedMessage}";
                return true;
            }

            if (SqlDataHelper.IsSqlTimeout(sqlException))
            {
                message = $"Database operation timed out: {normalizedMessage}";
                return true;
            }

            message = normalizedMessage;
            return true;
        }

        if (exception.InnerException is not null)
        {
            return TryBuildSqlFailureMessage(exception.InnerException, out message);
        }

        message = string.Empty;
        return false;
    }

    private static string NormalizeMessage(string message)
    {
        return message
            .Replace("\r\n", " ", StringComparison.Ordinal)
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
    }

    private static string? TryExtractStoredProcedureName(string message)
    {
        const string prefix = "Could not find stored procedure '";
        var start = message.IndexOf(prefix, StringComparison.OrdinalIgnoreCase);
        if (start < 0)
        {
            return null;
        }

        start += prefix.Length;
        var end = message.IndexOf('\'', start);
        if (end <= start)
        {
            return null;
        }

        return message[start..end].Trim();
    }
}