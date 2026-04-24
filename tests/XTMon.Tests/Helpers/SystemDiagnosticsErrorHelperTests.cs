using Microsoft.Data.SqlClient;
using XTMon.Helpers;

namespace XTMon.Tests.Helpers;

public class SystemDiagnosticsErrorHelperTests
{
    [Fact]
    public void BuildFailureMessage_WhenStoredProcedureIsMissing_ReturnsProcedureName()
    {
        var exception = MakeSqlException(2812, "Could not find stored procedure 'administration.UspFailRunningReplayBatches'.");

        var message = SystemDiagnosticsErrorHelper.BuildFailureMessage(exception, "fallback");

        Assert.Equal("Stored procedure administration.UspFailRunningReplayBatches is missing in the target database.", message);
    }

    [Fact]
    public void BuildFailureMessage_WhenConnectionFails_ReturnsConnectionMessage()
    {
        var exception = MakeSqlException(53, "A network-related or instance-specific error occurred while establishing a connection to SQL Server.");

        var message = SystemDiagnosticsErrorHelper.BuildFailureMessage(exception, "fallback");

        Assert.StartsWith("Database connection failed:", message, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildFailureMessage_WhenTimeoutOccurs_ReturnsTimeoutMessage()
    {
        var exception = MakeSqlException(-2, "Execution Timeout Expired.");

        var message = SystemDiagnosticsErrorHelper.BuildFailureMessage(exception, "fallback");

        Assert.Equal("Database operation timed out: Execution Timeout Expired.", message);
    }

    [Fact]
    public void BuildFailureMessage_WhenInnerSqlExceptionExists_UsesInnerException()
    {
        var exception = new InvalidOperationException("wrapper", MakeSqlException(2812, "Could not find stored procedure 'administration.UspGetStuckReplayBatches'."));

        var message = SystemDiagnosticsErrorHelper.BuildFailureMessage(exception, "fallback");

        Assert.Equal("Stored procedure administration.UspGetStuckReplayBatches is missing in the target database.", message);
    }

    [Fact]
    public void BuildFailureMessage_WhenExceptionIsNotSql_ReturnsFallback()
    {
        var message = SystemDiagnosticsErrorHelper.BuildFailureMessage(new InvalidOperationException("boom"), "fallback");

        Assert.Equal("fallback", message);
    }

    private static SqlException MakeSqlException(int number, string message)
    {
        const System.Reflection.BindingFlags nonPublicInstance =
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;

        var errorCtor = typeof(SqlError).GetConstructors(nonPublicInstance)[0];
        var errorArgs = BuildArgs(errorCtor.GetParameters(), new Dictionary<Type, object?>
        {
            [typeof(int)] = number,
            [typeof(byte)] = (byte)0,
            [typeof(string)] = message,
            [typeof(uint)] = (uint)0,
            [typeof(Exception)] = null,
        });
        var error = (SqlError)errorCtor.Invoke(errorArgs);

        var collectionCtor = typeof(SqlErrorCollection).GetConstructors(nonPublicInstance)[0];
        var errors = (SqlErrorCollection)collectionCtor.Invoke(Array.Empty<object>());
        var addMethod = typeof(SqlErrorCollection).GetMethod("Add", nonPublicInstance)!;
        addMethod.Invoke(errors, new object[] { error });

        var exceptionCtor = typeof(SqlException).GetConstructors(nonPublicInstance)[0];
        var exceptionArgs = BuildArgs(exceptionCtor.GetParameters(), new Dictionary<Type, object?>
        {
            [typeof(string)] = message,
            [typeof(SqlErrorCollection)] = errors,
            [typeof(Exception)] = null,
            [typeof(Guid)] = Guid.NewGuid(),
        });

        return (SqlException)exceptionCtor.Invoke(exceptionArgs);
    }

    private static object?[] BuildArgs(System.Reflection.ParameterInfo[] parameters, Dictionary<Type, object?> overrides)
    {
        var args = new object?[parameters.Length];
        var typeUsageCount = new Dictionary<Type, int>();

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameterType = parameters[i].ParameterType;
            typeUsageCount.TryGetValue(parameterType, out var usageIndex);
            typeUsageCount[parameterType] = usageIndex + 1;

            if (overrides.TryGetValue(parameterType, out var overrideValue))
            {
                args[i] = usageIndex == 0 ? overrideValue : GetDefault(parameterType);
                continue;
            }

            args[i] = GetDefault(parameterType);
        }

        return args;
    }

    private static object? GetDefault(Type type)
    {
        if (type == typeof(string)) return string.Empty;
        if (type == typeof(byte)) return (byte)0;
        if (type == typeof(short)) return (short)0;
        if (type == typeof(int)) return 0;
        if (type == typeof(uint)) return (uint)0;
        if (type == typeof(long)) return 0L;
        if (type == typeof(bool)) return false;
        if (type == typeof(Guid)) return Guid.Empty;
        if (type.IsValueType) return Activator.CreateInstance(type);
        return null;
    }
}