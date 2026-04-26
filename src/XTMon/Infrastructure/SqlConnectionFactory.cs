using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System.Data;
using XTMon.Options;

namespace XTMon.Infrastructure;

public sealed class SqlConnectionFactory
{
    private readonly IConfiguration _configuration;
    private readonly string _monitoringJobDatabaseName;
    private readonly string _monitoringJobSetExecutionContextStoredProcedure;
    private readonly SqlExecutionContextAccessor _sqlExecutionContextAccessor;

    public SqlConnectionFactory(
        IConfiguration configuration,
        SqlExecutionContextAccessor sqlExecutionContextAccessor,
        IOptions<MonitoringJobsOptions> monitoringJobsOptions)
    {
        _configuration = configuration;
        _sqlExecutionContextAccessor = sqlExecutionContextAccessor;

        var monitoringJobOptions = monitoringJobsOptions.Value;
        _monitoringJobSetExecutionContextStoredProcedure = monitoringJobOptions.JobSetExecutionContextStoredProcedure;

        var monitoringJobConnectionString = configuration.GetConnectionString(monitoringJobOptions.JobConnectionStringName);
        if (string.IsNullOrWhiteSpace(monitoringJobConnectionString))
        {
            throw new InvalidOperationException($"Connection string '{monitoringJobOptions.JobConnectionStringName}' is not configured.");
        }

        var connectionStringBuilder = new SqlConnectionStringBuilder(monitoringJobConnectionString);
        _monitoringJobDatabaseName = connectionStringBuilder.InitialCatalog;
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
        if (!string.Equals(connection.Database, _monitoringJobDatabaseName, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = _monitoringJobSetExecutionContextStoredProcedure;
        command.CommandType = CommandType.StoredProcedure;
        command.CommandTimeout = 5;

        var context = _sqlExecutionContextAccessor.CurrentContext;
        command.Parameters.Add(new SqlParameter("@JobId", SqlDbType.BigInt)
        {
            Value = context is null ? DBNull.Value : context.JobId
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
