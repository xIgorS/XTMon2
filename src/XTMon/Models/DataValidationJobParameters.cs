namespace XTMon.Models;

public sealed record DataValidationJobParameters(
    string? SourceSystemCodes = null,
    bool? TraceAllVersions = null,
    decimal? Precision = null);