using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Http;
using XTMon.Helpers;
using XTMon.Repositories;

namespace XTMon.Infrastructure;

public static class MonitoringExportEndpoint
{
    public static async Task HandleAsync(HttpContext context, IMonitoringJobRepository monitoringJobRepository)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(monitoringJobRepository);

        if (!TryReadQuery(context.Request.Query, out var category, out var submenuKey, out var pnlDate))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        var latestJob = await monitoringJobRepository.GetLatestMonitoringJobAsync(category, submenuKey, pnlDate, context.RequestAborted);
        if (latestJob is null || !latestJob.SavedAt.HasValue)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/csv; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        context.Response.Headers.ContentDisposition = string.Create(
            CultureInfo.InvariantCulture,
            $"attachment; filename=\"{BuildFileName(category, submenuKey, pnlDate)}\"");

        try
        {
            await monitoringJobRepository.StreamFullResultCsvAsync(
                latestJob.PersistedResultJobId ?? latestJob.JobId,
                context.Response.Body,
                context.RequestAborted);
        }
        catch (MonitoringJobFullResultNotFoundException)
        {
            if (!context.Response.HasStarted)
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
            }
        }
    }

    private static bool TryReadQuery(IQueryCollection query, out string category, out string submenuKey, out DateOnly pnlDate)
    {
        category = string.Empty;
        submenuKey = query["submenuKey"].ToString().Trim();
        pnlDate = default;

        var rawCategory = query["category"].ToString().Trim();
        if (string.Equals(rawCategory, MonitoringJobHelper.DataValidationCategory, StringComparison.OrdinalIgnoreCase))
        {
            category = MonitoringJobHelper.DataValidationCategory;
        }
        else if (string.Equals(rawCategory, MonitoringJobHelper.FunctionalRejectionCategory, StringComparison.OrdinalIgnoreCase))
        {
            category = MonitoringJobHelper.FunctionalRejectionCategory;
        }
        else
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(submenuKey))
        {
            return false;
        }

        return DateOnly.TryParseExact(
            query["pnlDate"].ToString(),
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out pnlDate);
    }

    private static string BuildFileName(string category, string submenuKey, DateOnly pnlDate)
    {
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{SanitizeFileNameSegment(category)}-{SanitizeFileNameSegment(submenuKey)}-{pnlDate:yyyy-MM-dd}.csv");
    }

    private static string SanitizeFileNameSegment(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (var character in value.Trim())
        {
            builder.Append(char.IsLetterOrDigit(character) || character is '-' or '_'
                ? character
                : '-');
        }

        return builder.Length == 0 ? "result" : builder.ToString();
    }
}