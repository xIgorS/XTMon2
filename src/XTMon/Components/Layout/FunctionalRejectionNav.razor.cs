using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.WebUtilities;
using XTMon.Infrastructure;
using XTMon.Models;
using XTMon.Repositories;

namespace XTMon.Components.Layout;

public partial class FunctionalRejectionNav : ComponentBase, IDisposable
{
    private readonly List<FunctionalRejectionMenuItem> menuItems = [];

    [Inject]
    private IFunctionalRejectionRepository Repository { get; set; } = default!;

    [Inject]
    private NavigationManager NavigationManager { get; set; } = default!;

    [Inject]
    private ILogger<FunctionalRejectionNav> Logger { get; set; } = default!;

    [Parameter]
    public bool IsOpen { get; set; }

    private bool isLoading;
    private string? loadError;

    protected override async Task OnInitializedAsync()
    {
        NavigationManager.LocationChanged += OnLocationChanged;
        await LoadMenuItemsAsync();
    }

    public void Dispose()
    {
        NavigationManager.LocationChanged -= OnLocationChanged;
    }

    private async Task LoadMenuItemsAsync()
    {
        isLoading = true;
        loadError = null;

        try
        {
            var items = await Repository.GetMenuItemsAsync(CancellationToken.None);
            menuItems.Clear();
            menuItems.AddRange(items);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                AppLogEvents.MonitoringLoadFailed,
                ex,
                "Failed to load Functional Rejection navigation items.");
            loadError = "Unable to load Functional Rejection items right now.";
            menuItems.Clear();
        }
        finally
        {
            isLoading = false;
        }
    }

    private void OnLocationChanged(object? sender, Microsoft.AspNetCore.Components.Routing.LocationChangedEventArgs e)
    {
        _ = InvokeAsync(StateHasChanged);
    }

    private string GetItemHref(FunctionalRejectionMenuItem item)
    {
        return QueryHelpers.AddQueryString(
            "functional-rejection",
            new Dictionary<string, string?>
            {
                ["code"] = item.SourceSystemBusinessDataTypeCode,
                ["businessDatatypeId"] = item.BusinessDataTypeId.ToString(CultureInfo.InvariantCulture),
                ["sourceSystemName"] = item.SourceSystemName,
                ["dbConnection"] = item.DbConnection
            });
    }

    private string GetSubmenuLinkClass(FunctionalRejectionMenuItem item)
    {
        return IsActive(item)
            ? "submenu-link submenu-link-active"
            : "submenu-link";
    }

    private bool IsActive(FunctionalRejectionMenuItem item)
    {
        var relativePath = NavigationManager.ToBaseRelativePath(NavigationManager.Uri);
        var route = relativePath.Split('?', 2)[0].TrimEnd('/');
        if (!string.Equals(route, "functional-rejection", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var uri = NavigationManager.ToAbsoluteUri(NavigationManager.Uri);
        var query = QueryHelpers.ParseQuery(uri.Query);
        if (!query.TryGetValue("code", out var code) ||
            !query.TryGetValue("businessDatatypeId", out var businessDatatypeId) ||
            !query.TryGetValue("sourceSystemName", out var sourceSystemName) ||
            !query.TryGetValue("dbConnection", out var dbConnection))
        {
            return false;
        }

        return string.Equals(code.ToString(), item.SourceSystemBusinessDataTypeCode, StringComparison.OrdinalIgnoreCase)
            && string.Equals(sourceSystemName.ToString(), item.SourceSystemName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(dbConnection.ToString(), item.DbConnection, StringComparison.OrdinalIgnoreCase)
            && string.Equals(businessDatatypeId.ToString(), item.BusinessDataTypeId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
    }
}