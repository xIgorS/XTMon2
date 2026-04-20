namespace XTMon.Models;

public sealed record TechnicalRejectResult(
    string ParsedQuery,
    IReadOnlyList<TechnicalRejectColumn> Columns,
    IReadOnlyList<IReadOnlyList<string?>> Rows);