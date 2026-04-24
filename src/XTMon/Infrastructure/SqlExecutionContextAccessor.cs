using System.Threading;
using XTMon.Models;

namespace XTMon.Infrastructure;

public sealed class SqlExecutionContextAccessor
{
    private static readonly AsyncLocal<SqlExecutionContextHolder?> Current = new();

    public SqlExecutionContext? CurrentContext => Current.Value?.Context;

    public IDisposable BeginMonitoringJobScope(MonitoringJobRecord job)
    {
        var previous = Current.Value;
        Current.Value = new SqlExecutionContextHolder(
            new SqlExecutionContext(job.JobId, job.Category, job.SubmenuKey),
            previous);

        return new RestoreScope(previous);
    }

    private sealed record SqlExecutionContextHolder(SqlExecutionContext Context, SqlExecutionContextHolder? Previous);

    private sealed class RestoreScope : IDisposable
    {
        private readonly SqlExecutionContextHolder? _previous;
        private bool _disposed;

        public RestoreScope(SqlExecutionContextHolder? previous)
        {
            _previous = previous;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            Current.Value = _previous;
            _disposed = true;
        }
    }
}

public sealed record SqlExecutionContext(long JobId, string Category, string SubmenuKey);