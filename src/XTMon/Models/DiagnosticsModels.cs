namespace XTMon.Models;

public sealed record StoredProcedureParameterInfo(
    string Name,
    string TypeName,
    bool IsOutput);

public sealed record StoredProcedureCheckResult(
    string FullName,
    bool Exists,
    IReadOnlyList<StoredProcedureParameterInfo> Parameters,
    string? Error);

public sealed record DatabaseCheckResult(
    string ConnectionStringName,
    string? ServerName,
    string? DatabaseName,
    bool Connected,
    TimeSpan Duration,
    string? ConnectionError,
    IReadOnlyList<StoredProcedureCheckResult> StoredProcedures);

public sealed record DiagnosticsReport(
    DateTimeOffset GeneratedAt,
    IReadOnlyList<DatabaseCheckResult> Databases)
{
    public bool AllPassed =>
        Databases.All(d => d.Connected && d.StoredProcedures.All(sp => sp.Exists && sp.Error is null));

    public int IssueCount =>
        Databases.Count(d => !d.Connected) +
        Databases.SelectMany(d => d.StoredProcedures).Count(sp => !sp.Exists || sp.Error is not null);
}
