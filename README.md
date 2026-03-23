# XTMon — APS Actions XTarget Monitoring

A central monitoring and recovery dashboard for SQL Server processing. Provides real-time visibility into database infrastructure health, allows operation teams to replay failed ETL flows, and facilitates JV calculation checks.

## Repository Structure

```text
XTMon2/
├── src/
│   └── XTMon/
│       ├── Components/
│       │   ├── Layout/          # Main layout, navigation, reconnect modal
│       │   ├── Pages/           # Routable Blazor pages
│       │   └── Shared/          # Reusable UI components
│       ├── Helpers/             # Pure logic helpers used by pages/services
│       ├── Infrastructure/      # Logging, connection factory, event IDs
│       ├── Models/              # DTOs and result records
│       ├── Options/             # Strongly-typed configuration objects
│       ├── Repositories/        # SQL Server data-access layer
│       ├── Security/            # UAM authorization requirement/handler/state
│       ├── Services/            # Hosted services and diagnostics services
│       ├── Sql/                 # SQL scripts / database artifacts
│       ├── Styles/              # Tailwind source styles
│       ├── wwwroot/             # Static assets and compiled CSS
│       ├── Program.cs           # Application bootstrap and DI wiring
│       └── XTMon.csproj         # Main web application project
├── tests/
│   └── XTMon.Tests/
│       ├── Helpers/             # Helper unit tests
│       ├── Queue/               # Replay queue tests
│       ├── Security/            # Authorization handler tests
│       ├── Services/            # Hosted service tests
│       └── XTMon.Tests.csproj   # xUnit test project
├── azure-pipelines.yml
├── global.json
├── README.md
├── SystemArchitecture.md
└── XTMon.sln
```

## Prerequisites

- **Development:** .NET 10 SDK
- **IIS deployment:** [.NET 10 **Hosting Bundle**](https://dotnet.microsoft.com/download/dotnet/10.0) — installs `AspNetCoreModuleV2` into IIS; the SDK or Runtime alone is **not** sufficient
- SQL Server access (connection strings configured in `appsettings.json`)
- Windows Authentication (Negotiate/NTLM)
- **Running tests:** no extra prerequisites — tests run without a live database or browser

## Running Tests

```powershell
dotnet test tests/XTMon.Tests/XTMon.Tests.csproj
```

The test project (`tests/XTMon.Tests/`) currently contains 175 xUnit unit tests covering pure logic helpers, monitoring display formatting, the replay wake-up queue, background service orchestration, and the UAM authorization handler. No SQL Server connection or browser is needed.

See the [Testing](#testing) section for coverage details.

### Test Project Usage

The test project is self-contained and can be run independently of the web app.

```powershell
dotnet test .\tests\XTMon.Tests\XTMon.Tests.csproj
```

Useful variants:

```powershell
# Run the full suite
dotnet test .\tests\XTMon.Tests\XTMon.Tests.csproj

# Run only one test class
dotnet test .\tests\XTMon.Tests\XTMon.Tests.csproj --filter "FullyQualifiedName~ReplayFlowProcessingQueueTests"

# Run only security-related tests
dotnet test .\tests\XTMon.Tests\XTMon.Tests.csproj --filter "FullyQualifiedName~UamPermissionHandlerTests"
```

The test project is organized by production layer:

- `tests/XTMon.Tests/Helpers` validates pure helper logic.
- `tests/XTMon.Tests/Queue` validates replay processing queue semantics.
- `tests/XTMon.Tests/Services` validates hosted-service orchestration.
- `tests/XTMon.Tests/Security` validates authorization handler behavior.

## Running Locally

### Development Mode

```powershell
dotnet run --project .\src\XTMon\XTMon.csproj
```

- Uses the **`https`** launch profile (port **7009**)
- Loads `appsettings.json` + `appsettings.Development.json`
- UAM authorization is **bypassed** — only Windows authentication is required
- Environment indicator shows **DEV** (amber badge)

Open: **https://localhost:7009**

Typical local workflow:

1. Start the app in Development mode.
2. Open the Overview page.
3. Use **Database Space** and **Database Backups** for monitoring data.
4. Use **Replay Flows** to load failed flow rows, select items, and submit replay requests.
5. Use **JV Calculation** to enqueue a check or fix-and-check job, then wait for the status panel and result grid to refresh.

### Production Mode

```powershell
dotnet run --project .\src\XTMon\XTMon.csproj --launch-profile https-prod
```

- Uses the **`https-prod`** launch profile (port **7010**)
- Loads `appsettings.json` only
- UAM authorization is **enforced** — users must have Name "APS" in the UAM database
- Environment indicator shows **PROD** (rose badge)

Open: **https://localhost:7010**

> In Visual Studio, select **https-prod** from the launch profile dropdown in the toolbar.

## Environment Indicator

A small badge in the bottom-left corner of the sidebar shows the current mode:

| Badge | Color | Meaning |
|-------|-------|---------|
| **DEV** | Amber/Yellow | Development — authorization bypassed |
| **PROD** | Rose/Pink | Production — UAM authorization enforced |

## Deploying to IIS (Windows Server)

> **IMPORTANT — Read before deploying:**  
> The **.NET 10 Hosting Bundle** is a mandatory prerequisite for IIS deployment. It is **not** the same as the .NET 10 SDK or Runtime alone.
> Installing only the SDK or Runtime on the server will cause IIS to fail silently — `web.config` will be unreadable to IIS, authentication settings will show "Retrieving status…" and never load, the site will return `500` or `401` errors, and no stdout log will be created.
> The Hosting Bundle installs `AspNetCoreModuleV2`, which IIS requires to host any ASP.NET Core application.

### Step 1: Install the .NET 10 Hosting Bundle

On the Windows Server, download and install the [.NET 10 **Hosting Bundle**](https://dotnet.microsoft.com/download/dotnet/10.0) — not the SDK, not the Runtime alone.

The Hosting Bundle installs:
- The ASP.NET Core Module V2 (`AspNetCoreModuleV2`) into IIS
- The .NET runtime required to run the app

After install, restart IIS:

```powershell
iisreset
```

Verify `AspNetCoreModuleV2` is registered:

```powershell
& "$env:windir\system32\inetsrv\appcmd.exe" list modules | findstr /I AspNetCore
```

You should see `AspNetCoreModuleV2` in the output. If it is missing, repair or re-install the Hosting Bundle.

### Step 2: Publish the Application

On your development machine:

```powershell
dotnet publish .\src\XTMon\XTMon.csproj -c Release -o .\publish
```

The `publish` directory is generated output and should be treated as disposable deployment artifacts rather than source-controlled project files.

### Step 3: Copy to the Server

Copy the `publish/` folder contents to the IIS site directory, for example:

```
C:\inetpub\XTMon\
```

### Step 4: Create the IIS Site

1. Open **IIS Manager**
2. Right-click **Sites** → **Add Website**
3. Set:
   - **Site name:** `XTMon`
   - **Physical path:** `C:\inetpub\XTMon`
   - **Binding:** choose the port and HTTPS certificate as needed (e.g., for the AVI load balancer URL)
4. Set the **Application Pool**:
   - Select the app pool → **Advanced Settings**
   - Set **.NET CLR Version** to **No Managed Code** (the ASP.NET Core Module handles the runtime)
   - **Identity:** For Kerberos delegation, change the built-in `ApplicationPoolIdentity` to a dedicated Active Directory Service Account (e.g., `DOMAIN\ServiceAccount`).

### Step 5: Enable Windows Authentication

1. Select the XTMon site in IIS Manager
2. Open **Authentication**
3. **Enable:** Windows Authentication
4. **Disable:** Anonymous Authentication
5. Right-click **Windows Authentication** → **Providers**:
   - Ensure **Negotiate** is at the top of the list.
6. Right-click **Windows Authentication** → **Advanced Settings**:
   - Disable **Enable Kernel-mode authentication** (required when using a custom App Pool Identity).
   - Check **Use App Pool Credentials** (so IIS can decrypt the Kerberos ticket using the Service Account's SPN).

### Step 5.1: Kerberos Delegation & AVI Load Balancer Setup

When deploying XTMon behind an AVI load balancer with a custom URL, you must configure Service Principal Names (SPNs) and Active Directory delegation for Windows Authentication to function correctly.

1. **Register SPNs (`setspn`)**
   The Service Account running the IIS Application Pool must have HTTP SPNs registered for the load-balanced URL. Run these commands as a domain admin:
   ```powershell
   setspn -S HTTP/xtmon-url DOMAIN\ServiceAccount
   setspn -S HTTP/xtmon-url.company.com DOMAIN\ServiceAccount
   ```
   > **Note on non-standard ports:** For standard ports (80/443), the port number should be omitted. If the URL uses a non-standard port (e.g., 8443), you should register SPNs both *with* and *without* the port number appended (e.g., `HTTP/xtmon-url:8443`) to ensure compatibility across all browsers.
2. **Active Directory Delegation**
   Ensure the Service Account (`DOMAIN\ServiceAccount`) is configured in Active Directory to allow delegation. This is necessary for the application to authenticate users via the custom URL and correctly pass identity to backend resources like SQL Server if `Integrated Security` is used.

### Step 6: Set the Environment (Dev vs Prod)

Edit `web.config` in the published folder to control the mode:

**Production deployment:**
```xml
<configuration>
  <location path="." inheritInChildApplications="false">
    <system.webServer>
      <aspNetCore processPath=".\XTMon.exe" stdoutLogEnabled="false" stdoutLogFile=".\logs\stdout" hostingModel="inprocess">
        <environmentVariables>
          <environmentVariable name="ASPNETCORE_ENVIRONMENT" value="Production" />
        </environmentVariables>
      </aspNetCore>
    </system.webServer>
  </location>
</configuration>
```

**Development/staging deployment:**
```xml
        <environmentVariables>
          <environmentVariable name="ASPNETCORE_ENVIRONMENT" value="Development" />
        </environmentVariables>
```

### Step 7: Update Connection Strings

Edit `appsettings.json` in the published folder to point to the target SQL Server:

```json
"ConnectionStrings": {
    "LogFiAlmt": "Server=PROD_SQL;Database=LOG_FI_ALMT;Integrated Security=true;TrustServerCertificate=True;",
    "StagingFiAlmt": "Server=PROD_SQL;Database=STAGING_FI_ALMT;Integrated Security=true;TrustServerCertificate=True;",
    "Publication": "Server=PROD_SQL;Database=PUBLICATION;Integrated Security=true;TrustServerCertificate=True;",
    "DtmFi": "Server=PROD_SQL;Database=DTM_FI;Integrated Security=true;TrustServerCertificate=True;",
    "MainUam": "Server=PROD_SQL;Database=MAIN_UAM;Integrated Security=true;TrustServerCertificate=True;"
}
```

> **Tip:** Use `Integrated Security=true` so the app connects to SQL Server using the IIS App Pool identity.

### What IIS Handles vs What appsettings.json Handles

| Concern | Configured in |
|---------|---------------|
| Port, HTTPS, SSL certificate | **IIS** (site bindings) |
| Windows Authentication | **IIS** (authentication settings) |
| Dev vs Prod mode | **web.config** (`ASPNETCORE_ENVIRONMENT`) |
| Connection strings | **appsettings.json** |
| Stored procedure names, UAM config | **appsettings.json** |
| Logging | **appsettings.json** |

## Quick Deploy to IIS in Development Mode

Use this when you want XTMon hosted by IIS but running with `ASPNETCORE_ENVIRONMENT=Development`.

### 1) Prerequisites on Server

- IIS installed
- [.NET 10 Hosting Bundle](https://dotnet.microsoft.com/download/dotnet/10.0) installed (required for `AspNetCoreModuleV2`)

### 2) Publish in Debug

From project root:

```powershell
dotnet publish .\src\XTMon\XTMon.csproj -c Debug -o C:\inetpub\XTMon-Dev
```

### 3) Create IIS App Pool + Site

1. Create app pool (for example `XTMon-Dev`) and set:
   - **.NET CLR Version:** `No Managed Code`
   - **Managed pipeline mode:** `Integrated`
2. Create website (for example `XTMon-Dev`) and point physical path to:
   - `C:\inetpub\XTMon-Dev`
3. Assign the site to the `XTMon-Dev` app pool.

### 4) Set Development Environment in IIS

In IIS Manager:

1. Select the site
2. Open **Configuration Editor**
3. Path: `system.webServer/aspNetCore`
4. Add environment variable:
   - `ASPNETCORE_ENVIRONMENT` = `Development`

Or with `appcmd`:

```powershell
& "$env:windir\system32\inetsrv\appcmd.exe" set config "XTMon-Dev" -section:system.webServer/aspNetCore /+"environmentVariables.[name='ASPNETCORE_ENVIRONMENT',value='Development']" /commit:apphost
```

### 5) Folder Permissions

Grant the app pool identity read/execute access to `C:\inetpub\XTMon-Dev`.
If writing logs/files, also grant write permission to the target folders.

### 6) Recycle and Verify

```powershell
iisreset
```

Then browse the site and confirm DEV behavior (DEV badge + development settings loaded).

## JV Troubleshooting Logs

Use these event IDs to diagnose JV timeout and SQL connectivity issues.

### Repository/Data-Access Events (3000 range)

| Event ID | Name | Meaning |
|----------|------|---------|
| 3007 | `RepositoryJvSqlTimeout` | SQL command timeout during JV repository operation (including command/procedure context). |
| 3008 | `RepositoryJvConnectionFailed` | SQL connectivity/login/network failure during JV repository operation. |
| 3009 | `RepositoryJvSlowOperation` | JV repository operation exceeded slow-operation threshold (currently 5000 ms). |

### JV Background Processor Events (4000 range)

| Event ID | Name | Meaning |
|----------|------|---------|
| 4002 | `JvProcessorSqlTimeout` | SQL timeout surfaced inside JV background processing loop/job execution. |
| 4003 | `JvProcessorConnectionFailed` | SQL connectivity/login/network issue surfaced inside JV background processing loop/job execution. |

### Where to Look

- File logs: `logs/xtmon-*.log`
- Database log sink (warnings/errors): `[monitoring].[UspInsertAPSActionsLog]` in `LOG_FI_ALMT` (if enabled)
- Serilog internal sink diagnostics: `logs/serilog-selflog.log`

### APSActions Log Sink Behavior

- Warning/Error events are written to `LOG_FI_ALMT` through `[monitoring].[UspInsertAPSActionsLog]`.
- If that stored procedure call fails (permissions/schema mismatch/temporary SQL issue), XTMon writes sink diagnostics to `logs/serilog-selflog.log`.
- XTMon no longer uses a direct table insert fallback, so runtime SQL logging remains stored-procedure-only.

Use this when troubleshooting "error shown in UI but nothing in APSActionsLogs" on another PC.

### Quick Filters

PowerShell filter for JV timeout/connection events in local file logs:

```powershell
Select-String -Path .\logs\xtmon-*.log -Pattern "RepositoryJvSqlTimeout|RepositoryJvConnectionFailed|JvProcessorSqlTimeout|JvProcessorConnectionFailed|RepositoryJvSlowOperation"
```

SQL filter for JV timeout/connection events in `LOG_FI_ALMT`:

```sql
DECLARE @SinceUtc DATETIME2(3) = DATEADD(HOUR, -24, SYSUTCDATETIME());

SELECT
      l.[TimeStamp],
      l.Level,
      l.Message,
      l.MessageTemplate,
      l.Properties,
      l.Exception
FROM [monitoring].[APSActionsLogs] l
WHERE l.[TimeStamp] >= @SinceUtc
   AND (
          l.[Message] LIKE '%RepositoryJvSqlTimeout%'
      OR l.[Message] LIKE '%RepositoryJvConnectionFailed%'
      OR l.[Message] LIKE '%RepositoryJvSlowOperation%'
      OR l.[Message] LIKE '%JvProcessorSqlTimeout%'
      OR l.[Message] LIKE '%JvProcessorConnectionFailed%'
      OR l.[Properties] LIKE '%RepositoryJvSqlTimeout%'
      OR l.[Properties] LIKE '%RepositoryJvConnectionFailed%'
      OR l.[Properties] LIKE '%RepositoryJvSlowOperation%'
      OR l.[Properties] LIKE '%JvProcessorSqlTimeout%'
      OR l.[Properties] LIKE '%JvProcessorConnectionFailed%'
      OR l.[Properties] LIKE '%EventId%3007%'
      OR l.[Properties] LIKE '%EventId%3008%'
      OR l.[Properties] LIKE '%EventId%3009%'
      OR l.[Properties] LIKE '%EventId%4002%'
      OR l.[Properties] LIKE '%EventId%4003%'
   )
ORDER BY l.[TimeStamp] DESC;
```

### JV Timeout Tuning

- `JvCalculation:CommandTimeoutSeconds`: SQL command timeout for JV procedures.
- `JvCalculation:JobPollIntervalSeconds`: UI polling interval for JV job status.
- `JvCalculation:JobRunningStaleTimeoutSeconds`: fail-safe timeout used to auto-fail stale `Running` jobs.

## Testing Authorization

| Environment | Behavior |
|-------------|----------|
| Development | All authenticated Windows users can access every page |
| Production  | Restricted pages require UAM Name "APS" from the `MAIN_UAM` database |

### Restricted Pages

- **Replay Flows** (`/replay-flows`)
- **JV Calculation Check** (`/jv-calculation-check`)

### How to Verify Authorization in Production

1. Open the app and check the sidebar — badge should show **PROD**
2. **Nav menu** — Replay Flows and JV Calculation links are **hidden** if your user lacks the UAM role
3. **Direct URL access** — navigating to `/replay-flows` returns **403 Forbidden**
4. Check logs in `logs/xtmon-*.log`:
   ```
   UamPermissionHandler: Checking UAM authorization for user DOMAIN\username
   UamPermissionHandler: User DOMAIN\username failed UAM authorization check.
   ```

### How Authorization Works

1. User authenticates via Windows Authentication (Negotiate/NTLM)
2. In Production, `UamPermissionHandler` calls `[uam].[UspGetAdminUserByBnpId]` on the `MAIN_UAM` database
3. If the user has **Name = "APS"**, access is granted; otherwise, access is denied
4. If the authorization backend is unavailable, the UI remains fail-closed and displays an **Access Unavailable** message instead of a generic authorization denial

## Configuration Files

| File | Purpose |
|------|---------|
| `src/XTMon/appsettings.json` | Base configuration (loaded in all environments) |
| `src/XTMon/appsettings.Development.json` | Development overrides (loaded only in Development mode) |
| `src/XTMon/Properties/launchSettings.json` | Launch profiles for local development |
| `web.config` | IIS integration and environment variable (auto-generated on publish) |

## Testing

The `XTMon.Tests` project is a self-contained xUnit test suite. It does not require a live SQL Server, IIS, or a browser to run.

```powershell
dotnet test tests/XTMon.Tests/XTMon.Tests.csproj
```

### What Is Covered

| Area | Tests | Notes |
|------|------:|-------|
| `SqlDataHelper` — type coercions, `ParseQuery`, SQL error classification | 41 | SQL exceptions constructed via reflection |
| `ReplayFlowsHelper` — flow set normalisation, all 12+ status strings, formatting | 52 | Pure logic, no dependencies |
| `MonitoringDisplayHelper` — display formatting for monitoring pages | 34 | Pure logic, no dependencies |
| `JvCalculationHelper` — stale detection, UTC conversion, header labels, JSON | 25 | Pure logic, no dependencies |
| `ReplayFlowProcessingQueue` — enqueue/dequeue, cancellation, coalesced wake-up signals | 6 | In-memory signal queue only |
| `ReplayFlowProcessingService` — item processing, transient error resilience | 3 | Mocked `IReplayFlowRepository` |
| `JvCalculationProcessingService` — CheckOnly vs FixAndCheck routing, failure handling, heartbeat ordering | 8 | Mocked `IJvCalculationRepository` |
| `UamPermissionHandler` — authorized / denied / unauthenticated / repository exception / feedback state reset | 6 | Mocked `IUamAuthorizationRepository` |

### What Is Not Covered (and Why)

| Layer | Reason |
|-------|--------|
| Blazor `.razor` markup | Requires a browser — no Playwright/Selenium |
| Repository SQL calls | Sealed ADO.NET types (`SqlConnection`, `SqlCommand`) require a real SQL Server |
| `StoredProcedureLogSink` | Tightly coupled to a live database |
| Windows Authentication | Requires OS-level setup |

### NuGet Packages (must be in internal feed)

`Microsoft.NET.Test.Sdk` · `xunit` · `xunit.runner.visualstudio` · `Moq` · `coverlet.collector` · `Microsoft.Extensions.Logging.Abstractions` · `Microsoft.Extensions.Options` · `Microsoft.AspNetCore.Authorization`

## Launch Profiles (Local Development)

| Profile | Port | Environment | Auth | Badge |
|---------|------|-------------|------|-------|
| `https` | 7009 | Development | Windows only | DEV (amber) |
| `https-prod` | 7010 | Production | Windows + UAM | PROD (rose) |
