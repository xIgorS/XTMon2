using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Http;
using XTMon.Services;

namespace XTMon.Components.Layout;

public partial class MainLayout : LayoutComponentBase, IDisposable
{
	private readonly StartupDiagnosticsGateController _gateController = new();
	private string? _authenticatedUserName;

	[Inject]
	private StartupDiagnosticsState StartupDiagnosticsState { get; set; } = default!;

	[Inject]
	private IHttpContextAccessor HttpContextAccessor { get; set; } = default!;

	protected override void OnInitialized()
	{
		_gateController.Initialize(StartupDiagnosticsState.HasCompleted);
		_authenticatedUserName = ResolveAuthenticatedUserName();

		StartupDiagnosticsState.StatusChanged += OnDiagnosticsStatusChanged;
	}

	protected override Task OnAfterRenderAsync(bool firstRender)
	{
		if (_gateController.ShouldTriggerStartupDiagnostics(firstRender))
		{
			_ = StartupDiagnosticsState.RunAsync(CancellationToken.None);
		}

		return Task.CompletedTask;
	}

	public void Dispose()
	{
		StartupDiagnosticsState.StatusChanged -= OnDiagnosticsStatusChanged;
	}

	private void OnDiagnosticsStatusChanged()
	{
		_ = InvokeAsync(async () =>
		{
			StateHasChanged();

			if (_gateController.TryReleaseGate(StartupDiagnosticsState.HasCompleted))
			{
				await Task.Yield();
				StateHasChanged();
			}
		});
	}

	private bool ShowApplicationBody() =>
		_gateController.ShowApplicationBody;

	private string GetStartupHeading()
	{
		if (StartupDiagnosticsState.IsRunning)
		{
			return "Running startup diagnostics";
		}

		if (StartupDiagnosticsState.HasCompleted)
		{
			return StartupDiagnosticsState.IsHealthy
				? "Diagnostics complete"
				: "Diagnostics completed with issues";
		}

		return "Preparing startup diagnostics";
	}

	private string GetStartupMessage()
	{
		if (StartupDiagnosticsState.IsRunning)
		{
			if (StartupDiagnosticsState.IsSlow)
			{
				return "Diagnostics are taking longer than expected. The app is still waiting for the startup checks to finish before unlocking.";
			}

			return "The application will unlock after database connections and stored procedure checks finish and the diagnostics badge is updated.";
		}

		if (StartupDiagnosticsState.HasCompleted)
		{
			return StartupDiagnosticsState.IsHealthy
				? "Diagnostics passed. Opening the application shell."
				: "Diagnostics found issues. The application shell will open with a red diagnostics badge so you can investigate.";
		}

		return "Diagnostics are queued and will begin immediately.";
	}

	private bool ShowStartupSlowRunNotice() =>
		StartupDiagnosticsState.IsRunning && StartupDiagnosticsState.IsSlow;

	private string GetStartupSlowRunNotice()
	{
		return "If this keeps happening, check SQL connectivity and stored procedure availability, then reload the page or open System Diagnostics after startup completes.";
	}

	private bool ShowStartupFailureSummary() =>
		StartupDiagnosticsState.HasCompleted && !StartupDiagnosticsState.IsHealthy;

	private string GetStartupFailureSummary()
	{
		if (!string.IsNullOrWhiteSpace(StartupDiagnosticsState.Error))
		{
			return StartupDiagnosticsState.Error!;
		}

		var report = StartupDiagnosticsState.Report;
		if (report is null)
		{
			return "Diagnostics completed with issues.";
		}

		var failedDatabase = report.Databases.FirstOrDefault(database => !database.Connected);
		if (failedDatabase is not null)
		{
			var databaseTarget = failedDatabase.DatabaseName is not null
				? $"{failedDatabase.ConnectionStringName} / {failedDatabase.DatabaseName}"
				: failedDatabase.ConnectionStringName;

			return $"Connection failed for {databaseTarget}: {failedDatabase.ConnectionError ?? "unknown connection error"}.";
		}

		foreach (var database in report.Databases)
		{
			var failedProcedure = database.StoredProcedures.FirstOrDefault(storedProcedure => !storedProcedure.Exists || storedProcedure.Error is not null);
			if (failedProcedure is null)
			{
				continue;
			}

			var problem = failedProcedure.Error ?? "stored procedure is missing";
			return $"{database.ConnectionStringName}: {failedProcedure.FullName} failed validation ({problem}).";
		}

		return "Diagnostics completed with issues.";
	}

	private string GetDiagnosticsBadgeClass()
	{
		var statusClass = StartupDiagnosticsState.IsRunning
			? "submenu-status-badge--running"
			: StartupDiagnosticsState.IsHealthy
				? "submenu-status-badge--succeeded"
				: StartupDiagnosticsState.HasCompleted
					? "submenu-status-badge--failed"
					: "submenu-status-badge--not-run";

		return $"submenu-status-badge {statusClass}";
	}

	private string GetDiagnosticsBadgeText()
	{
		if (StartupDiagnosticsState.IsRunning)
		{
			return "Diagnostics running";
		}

		if (StartupDiagnosticsState.IsHealthy)
		{
			return "Diagnostics OK";
		}

		if (StartupDiagnosticsState.HasCompleted)
		{
			return "Diagnostics issues";
		}

		return "Diagnostics pending";
	}

	private string GetDiagnosticsBadgeTitle()
	{
		if (StartupDiagnosticsState.IsRunning)
		{
			return "Startup diagnostics are currently running.";
		}

		if (StartupDiagnosticsState.IsHealthy && StartupDiagnosticsState.Report is not null)
		{
			return $"Diagnostics passed at {StartupDiagnosticsState.Report.GeneratedAt:dd-MM-yyyy HH:mm:ss}.";
		}

		if (!string.IsNullOrWhiteSpace(StartupDiagnosticsState.Error))
		{
			return StartupDiagnosticsState.Error;
		}

		if (StartupDiagnosticsState.Report is not null)
		{
			return $"Diagnostics found {StartupDiagnosticsState.Report.IssueCount} issue(s) at {StartupDiagnosticsState.Report.GeneratedAt:dd-MM-yyyy HH:mm:ss}.";
		}

		return "Startup diagnostics have not completed yet.";
	}

	private string GetAuthenticatedUserLabel()
	{
		return string.IsNullOrWhiteSpace(_authenticatedUserName)
			? "Authenticated user"
			: _authenticatedUserName;
	}

	private string? ResolveAuthenticatedUserName()
	{
		try
		{
			return HttpContextAccessor.HttpContext?.User?.Identity?.Name;
		}
		catch (ObjectDisposedException)
		{
			return null;
		}
		catch (InvalidOperationException)
		{
			return null;
		}
	}
}
