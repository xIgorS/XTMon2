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

Each repository has a corresponding interface in `src/XTMon/Repositories/`:
- `IReplayFlowRepository` — implemented by `ReplayFlowRepository`
- `IJvCalculationRepository` — implemented by `JvCalculationRepository`
- `IUamAuthorizationRepository` — implemented by `UamAuthorizationRepository`

Interfaces are registered in DI (`src/XTMon/Program.cs`) as `AddScoped<IInterface, Concrete>()`. Background services and the authorization handler depend on the interfaces, not the concrete classes, enabling unit testing with mocks.

### Internal Helper Classes

Pure-logic methods extracted from Blazor code-behind files into testable `internal static` classes in `src/XTMon/Helpers/`:
- `ReplayFlowsHelper` — `TryNormalizeReplayFlowSet`, `GetStatusKind`, `FormatDate`, `FormatNumber`, `FormatDuration`, plus the `ReplayStatusKind` enum
- `JvCalculationHelper` — `IsStaleRunningJob`, `ToUtc`, `ToHeaderLabel`, `GetColumnAlignmentClass`, `DeserializeMonitoringTable`

The test project accesses these via `[assembly: InternalsVisibleTo("XTMon.Tests")]` declared in `src/XTMon/XTMon.csproj`.

### Key Data Flow Patterns

**Monitoring/DbBackupInfo:** Blazor page → Repository → SQL Server stored procedure → return DTOs

**Replay Flows:** Blazor page → `ReplayFlowProcessingQueue` (in-memory `Channel<T>`) → `ReplayFlowProcessingService` (hosted service) → SQL Server

**JV Calculation Check:** Blazor page → `JvCalculationRepository` (enqueue job) → `JvCalculationProcessingService` (polls DB every 5s, maintains heartbeat) → result stored in DB → page polls for completion

### Authentication & Authorization

- **Authentication:** Windows Authentication via Negotiate (NTLM locally, Kerberos on IIS)
- **Development:** UAM check bypassed — any authenticated Windows user can access all pages
- **Production:** `RequiresUamPermission` policy enforced on `/replay-flows` and `/jv-calculation-check` — user must exist in `[uam].[UspGetAdminUserByBnpId]` with Name "APS"
- Environment is detected via `ASPNETCORE_ENVIRONMENT`; the sidebar shows a **DEV** (amber) or **PROD** (rose) badge

### Databases

Five SQL Server databases configured in `appsettings.json`:
| Key | Purpose |
|-----|---------|
| `StagingFiAlmt` | Replay flow source data |
| `LogFiAlmt` | Logging sink + JV job orchestration tables |
| `Publication` | JV calculation check source |
| `DtmFi` | JV fix operations |
| `MainUam` | UAM authorization |

All stored procedure names are configured as options (not hardcoded) and validated at startup.

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

**What is tested:**
| Area | File | Coverage |
|------|------|----------|
| `SqlDataHelper` | `Helpers/SqlDataHelperTests.cs` | All 8 methods; SQL error number classification via reflection-built `SqlException` |
| `ReplayFlowsHelper` | `Helpers/ReplayFlowsHelperTests.cs` | Replay flow set normalization, all 12+ status string mappings, date/number/duration formatting |
| `JvCalculationHelper` | `Helpers/JvCalculationHelperTests.cs` | Stale detection, UTC conversion, camelCase header labels, column alignment, JSON deserialization |
| `ReplayFlowProcessingQueue` | `Queue/ReplayFlowProcessingQueueTests.cs` | Enqueue/dequeue, cancellation, drop-on-full (capacity 10) |
| `ReplayFlowProcessingService` | `Services/ReplayFlowProcessingServiceTests.cs` | Item processing, error resilience (service continues after exception) |
| `JvCalculationProcessingService` | `Services/JvCalculationProcessingServiceTests.cs` | CheckOnly vs FixAndCheck routing, failure marking, stale expiry, heartbeat ordering |
| `UamPermissionHandler` | `Security/UamPermissionHandlerTests.cs` | Authorized/unauthorized/unauthenticated users, repository exception handling |

**What is not tested (and why):**
- Blazor `.razor` markup — requires a browser (no Playwright)
- Repository SQL calls — sealed ADO.NET types require a real SQL Server
- `StoredProcedureLogSink` — tightly coupled to live SQL
- Windows Authentication — requires OS-level setup

**NuGet packages** (must be available in internal feed):
`Microsoft.NET.Test.Sdk`, `xunit`, `xunit.runner.visualstudio`, `Moq`, `coverlet.collector`, `Microsoft.Extensions.Logging.Abstractions`, `Microsoft.Extensions.Options`, `Microsoft.AspNetCore.Authorization`

## IIS Deployment Note

IIS deployment requires the **.NET 10 Hosting Bundle** (not just the SDK or Runtime). Missing this causes silent failures (500/401 errors, no stdout log). After installing, run `iisreset`.
