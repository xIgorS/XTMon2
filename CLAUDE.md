# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**XTMon** (APS Actions XTarget Monitoring) is a Blazor Server web application (.NET 10, Windows-only) for SQL Server monitoring and ETL operations. It provides a dashboard for database health, replaying failed ETL flows, and running JV calculation checks.

## Build & Run Commands

```powershell
# Development (port 7009, UAM authorization bypassed)
dotnet run

# Production mode (port 7010, UAM authorization enforced)
dotnet run --launch-profile https-prod

# Publish for IIS
dotnet publish -c Release -o ./publish
dotnet publish -c Debug -o C:\inetpub\XTMon-Dev
```

```bash
# Tailwind CSS (must be rebuilt after changing component classes)
npm run build:css    # one-time build
npm run watch:css    # watch mode during development
```

There are no automated tests in this project.

## Architecture

The app follows a layered architecture:

- **Components/Pages/** — Blazor Server pages (routable, interactive server render mode)
- **Data/** — Repositories (SQL Server access) and background services (`IHostedService`)
- **Options/** — Strongly-typed `IOptions<T>` config classes bound from `appsettings.json`
- **Models/** — DTOs returned by repositories
- **Security/** — Custom `IAuthorizationHandler` for UAM role checks

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

Structured log event IDs are defined in `Data/AppLogEvents.cs` (range 3000–4000).

## Configuration

- `appsettings.json` — base config: connection strings, stored procedure names, timeouts
- `appsettings.Development.json` — dev overrides (UAM bypass)
- `Properties/launchSettings.json` — `https` (dev, port 7009) and `https-prod` (prod, port 7010)

## IIS Deployment Note

IIS deployment requires the **.NET 10 Hosting Bundle** (not just the SDK or Runtime). Missing this causes silent failures (500/401 errors, no stdout log). After installing, run `iisreset`.
