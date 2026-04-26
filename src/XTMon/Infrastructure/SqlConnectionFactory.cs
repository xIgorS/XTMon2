using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

namespace XTMon.Infrastructure;

public sealed class SqlConnectionFactory
{
    private readonly IConfiguration _configuration;
    private readonly SqlExecutionContextAccessor _sqlExecutionContextAccessor;

    public SqlConnectionFactory(
        IConfiguration configuration,
        SqlExecutionContextAccessor sqlExecutionContextAccessor)
    {
        _configuration = configuration;
        _sqlExecutionContextAccessor = sqlExecutionContextAccessor;
    }

    public SqlConnection CreateConnection()
    {
        return CreateConnection("LogFiAlmt");
    }

    public SqlConnection CreateConnection(string connectionStringName)
    {
        var connectionString = _configuration.GetConnectionString(connectionStringName);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException($"Connection string '{connectionStringName}' is not configured.");
        }

        return new SqlConnection(connectionString);
    }

    public async Task OpenAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await connection.OpenAsync(cancellationToken);
        await ApplyExecutionContextAsync(connection, cancellationToken);
    }

    private async Task ApplyExecutionContextAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = """
            DECLARE @Context varbinary(128) = 0x;

            IF @JobId IS NOT NULL
            BEGIN
                SET @Context = CONVERT(varbinary(128), 0x58544D4F4E4A4F42) + CONVERT(binary(8), @JobId);
            END

            SET CONTEXT_INFO @Context;
            """;
        command.CommandType = CommandType.Text;
        command.CommandTimeout = 5;

        var context = _sqlExecutionContextAccessor.CurrentContext;
        command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt)
        {
            Value = context is null ? DBNull.Value : context.JobId
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
