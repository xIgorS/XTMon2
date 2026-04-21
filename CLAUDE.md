# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**XTMon** (APS Actions XTarget Monitoring) is a Blazor Server web application (.NET 10, Windows-only) for SQL Server monitoring and ETL operations. It provides a dashboard for database health, replaying failed ETL flows, and running JV calculation checks.

## Build & Run Commands

```powershell
# Development (port 7009, UAM authorization bypassed)
dotnet run --project src/XTMon/XTMon.csproj

# Production mode (port 7010, UAM authorization enforced)
dotnet run --project src/XTMon/XTMon.csproj --launch-profile https-prod

# Publish for IIS
dotnet publish ./src/XTMon/XTMon.csproj -c Release -o ./publish
dotnet publish ./src/XTMon/XTMon.csproj -c Debug -o C:\inetpub\XTMon-Dev
```

```bash
# Tailwind CSS (must be rebuilt after changing component classes)
npm --prefix src/XTMon run build:css    # one-time build
npm --prefix src/XTMon run watch:css    # watch mode during development
```

```bash
# Run unit tests
dotnet test tests/XTMon.Tests/XTMon.Tests.csproj
```

## Architecture

The app follows a layered architecture:

- **src/XTMon/Components/Pages/** — Blazor Server pages (routable, interactive server render mode)
- **src/XTMon/Repositories/** — Repository interfaces and concrete implementations (SQL Server/ADO.NET)
- **src/XTMon/Services/** — Background/hosted services (`IHostedService`) and in-memory queues
- **src/XTMon/Helpers/** — Pure-logic `internal static` helper classes (testable without DB)
- **src/XTMon/Infrastructure/** — Cross-cutting concerns: `SqlConnectionFactory`, `StoredProcedureLogSink`, `AppLogEvents`
- **src/XTMon/Options/** — Strongly-typed `IOptions<T>` config classes bound from `appsettings.json`
- **src/XTMon/Models/** — DTOs returned by repositories
- **src/XTMon/Security/** — Custom `IAuthorizationHandler` for UAM role checks
- **tests/XTMon.Tests/** — xUnit unit test project (no live DB or browser required)

### Repository Interfaces

One `I<Feature>Repository` + `<Feature>Repository` pair per monitoring/data-validation page, plus `IReplayFlowRepository`, `IJvCalculationRepository`, `IMonitoringJobRepository`, `IUamAuthorizationRepository`. All registered in DI (`src/XTMon/Program.cs`) as `AddScoped<IInterface, Concrete>()`. Background services and the authorization handler depend on the interfaces (not the concrete classes), enabling unit testing with mocks.

When adding a new monitoring page, follow the existing pattern: new `I<Feature>Repository`/`<Feature>Repository` pair, `<Feature>Options` bound from `appsettings.json`, registration in `Program.cs`, and a `(ConnectionStringName, StoredProcedure)` entry in `DeploymentCheckService.BuildChecks` so startup validates it.

### Internal Helper Classes

Pure-logic methods extracted from Blazor code-behind into testable `internal static` classes in `src/XTMon/Helpers/`:
- `SqlDataHelper` — SQL reader helpers, `IsSqlTimeout`/`IsSqlConnectionFailure` classification
- `ReplayFlowsHelper` — replay flow set normalization, status mapping (`ReplayStatusKind` enum), date/number/duration formatting
- `JvCalculationHelper` — stale-job detection, UTC conversion, camelCase header labels, column alignment, JSON deserialization
- `MonitoringDisplayHelper` — shared display formatting for monitoring pages
- `MonitoringJobHelper` — serialization/parameterization for shared monitoring-job payloads
- `BatchStatusHelper`, `DataValidationBatchRunHelper`, `DataValidationCheckCatalog`, `DataValidationNavAlertHelper`, `PricingHelper` — per-feature display/logic helpers

The test project accesses these via `[assembly: InternalsVisibleTo("XTMon.Tests")]` declared in `src/XTMon/XTMon.csproj`.

### Key Data Flow Patterns

**Monitoring/DbBackupInfo:** Blazor page → Repository → SQL Server stored procedure → return DTOs

**Replay Flows:** Blazor page → `ReplayFlowProcessingQueue` (in-memory `Channel<T>`) → `ReplayFlowProcessingService` (hosted service) → SQL Server

**JV Calculation Check:** Blazor page → `JvCalculationRepository` (enqueue job) → `JvCalculationProcessingService` (polls DB every 5s, maintains heartbeat) → result stored in DB → page polls for completion

**Shared Monitoring Jobs (Batch Status, Data Validation, Functional Rejection, …):** Blazor page → `IMonitoringJobRepository.EnqueueAsync` → row in `LogFiAlmt` monitoring-jobs table → `MonitoringJobProcessingService` (hosted service, 5s idle poll) claims the next job → resolves a matching `IMonitoringJobExecutor` via `CanExecute(job)` (executors are scoped, multi-registered) → `ExecuteAsync` returns a `MonitoringJobResultPayload` → repository persists the result and marks complete/failed → page polls for completion. Stale "Running" jobs are auto-failed by the service on each tick using `MonitoringJobsOptions.JobRunningStaleTimeoutSeconds`.

To add a new monitoring-job-backed page, implement `IMonitoringJobExecutor` (see `BatchStatusMonitoringJobExecutor`, `DataValidationMonitoringJobExecutor`, `FunctionalRejectionMonitoringJobExecutor`) and register it with `AddScoped<IMonitoringJobExecutor, YourExecutor>()` — the processing service resolves all executors and dispatches by `CanExecute`.

### Scoped UI State Services

Registered as `AddScoped<T>()` so they share state within a Blazor circuit:
- `PnlDateState` — selected P&L date surfaced by `GlobalPnlDatePicker` in the sidebar, consumed by monitoring pages
- `DataValidationNavAlertState` — drives the red-dot badge on the Data Validation nav item
- `AuthorizationFeedbackState` — surfaces last UAM-denied action to the UI

### Authentication & Authorization

- **Authentication:** Windows Authentication via Negotiate (NTLM locally, Kerberos on IIS)
- **Policy:** `"UamRestricted"` — in Production it adds `RequiresUamPermissionRequirement` (checked against `[uam].[UspGetAdminUserByBnpId]`, user must have Name "APS"); in Development it only calls `RequireAuthenticatedUser()`, bypassing the DB check
- **Applied via `[Authorize(Policy = "UamRestricted")]`** on: `/replay-flows`, `/jv-calculation-check`, `/batch-status`, `/system-diagnostics`. `DataValidationRunner` invokes the policy imperatively via `IAuthorizationService` before running destructive actions. `Home.razor` uses `<AuthorizeView Policy="UamRestricted">` to conditionally render UAM-gated links
- Environment is detected via `ASPNETCORE_ENVIRONMENT`; the sidebar shows a **DEV** (amber) or **PROD** (rose) badge
- **Middleware order (`Program.cs`):** `UseAuthentication` → `UseAuthorization` → `UseStatusCodePagesWithReExecute`. Do not reorder — `StatusCodePages` placed before auth will swallow the 401 Negotiate challenge and break the NTLM handshake (there is an inline comment reminding of this)

### Databases

Five SQL Server databases configured in `appsettings.json`:
| Key | Purpose |
|-----|---------|
| `StagingFiAlmt` | Replay flow source data |
| `LogFiAlmt` | Logging sink + JV job orchestration tables |
| `Publication` | JV calculation check source |
| `DtmFi` | JV fix operations |
| `MainUam` | UAM authorization |

All stored procedure names are configured as options (not hardcoded) and validated at startup by `DeploymentCheckService` (singleton). The `/system-diagnostics` page runs the same check on demand: it connects to every configured DB and queries `sys.objects`/`sys.parameters` to verify each stored procedure exists and reports its signature. When adding a new stored procedure, add a `(ConnectionStringName, StoredProcedure)` entry to `DeploymentCheckService.BuildChecks` so it participates in this validation.

DDL for each database lives in `src/XTMon/Sql/` (`001_STAGING_FI_ALMT_Setup.sql`, `002_LOG_FI_ALMT_Logging_Setup.sql`, `003_LOG_FI_ALMT_JvJob_Orchestration.sql`, `004_LOG_FI_ALMT_MonitoringJob_Orchestration.sql`, and `<DB>.sql` snapshots). The `00N_*.sql` scripts are idempotent and safe to re-run.

### Logging

Serilog with two sinks:
- **SQL Server:** `monitoring.UspInsertAPSActionsLog` in `LogFiAlmt`
- **Rolling file:** `logs/xtmon-YYYYMMDD.log` (14-day retention)

Structured log event IDs are defined in `src/XTMon/Infrastructure/AppLogEvents.cs` (range 3000–4000).

## Configuration

- `src/XTMon/appsettings.json` — base config: connection strings, stored procedure names, timeouts
- `src/XTMon/appsettings.Development.json` — dev overrides (UAM bypass)
- `src/XTMon/Properties/launchSettings.json` — `https` (dev, port 7009) and `https-prod` (prod, port 7010)

## Testing

The `XTMon.Tests` project contains xUnit unit tests that run without a live SQL Server or browser.

**What is tested** (see `tests/XTMon.Tests/`):
- Helpers — `SqlDataHelper`, `ReplayFlowsHelper`, `MonitoringDisplayHelper`, `JvCalculationHelper`, `MonitoringJobHelper`, `BatchStatusHelper`, `DataValidationBatchRunHelper`, `DataValidationCheckCatalog`, `DataValidationNavAlertHelper`, `PricingHelper`
- Queue — `ReplayFlowProcessingQueue` (enqueue/dequeue, cancellation, drop-on-full at capacity 10)
- Services — `ReplayFlowProcessingService` (item processing, error resilience), `JvCalculationProcessingService` (CheckOnly vs FixAndCheck routing, failure marking, stale expiry, heartbeat ordering), `MonitoringJobProcessingService` (executor dispatch, stale expiry, failure marking), `DataValidationNavAlertState`
- Security — `UamPermissionHandler` (authorized/unauthorized/unauthenticated, repository exception handling)

Run a single test with `dotnet test tests/XTMon.Tests/XTMon.Tests.csproj --filter "FullyQualifiedName~<TestClass>.<TestMethod>"`.

**What is not tested (and why):**
- Blazor `.razor` markup — requires a browser (no Playwright)
- Repository SQL calls — sealed ADO.NET types require a real SQL Server
- `StoredProcedureLogSink` — tightly coupled to live SQL
- Windows Authentication — requires OS-level setup

**NuGet packages** (must be available in internal feed):
`Microsoft.NET.Test.Sdk`, `xunit`, `xunit.runner.visualstudio`, `Moq`, `coverlet.collector`, `Microsoft.Extensions.Logging.Abstractions`, `Microsoft.Extensions.Options`, `Microsoft.AspNetCore.Authorization`

## IIS Deployment Note

IIS deployment requires the **.NET 10 Hosting Bundle** (not just the SDK or Runtime). Missing this causes silent failures (500/401 errors, no stdout log). After installing, run `iisreset`.
