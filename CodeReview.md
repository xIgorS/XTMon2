# XTMon — Code Review Report

**Date:** 2026-03-10
**Stack:** .NET 10 Blazor Server · SQL Server · Serilog · Tailwind CSS · Windows Authentication
**Reviewer:** Claude Sonnet 4.6

**Overall impression:** Well-structured, clean, and production-aware. The layered architecture, immutable models, options validation, and logging design are all solid. The issues below range from outright bugs to design debt and security concerns.

---

## 🔴 Critical / Security

### 1. Plaintext SA credentials in `appsettings.json` committed to git

[appsettings.json](appsettings.json)

All five connection strings use `sa` / `DockerPassword123!` with `TrustServerCertificate=True`. Even if these are local dev values, they are committed to source control.

**Risks:**
- Secret rotation is impossible once committed; git history preserves them forever.
- `TrustServerCertificate=True` disables TLS certificate validation, opening the door to MITM attacks in any environment where the file is deployed as-is.
- `sa` is the most privileged SQL Server account. A breach of this credential is a full database compromise.

**Recommendations:**
- Use **Windows Integrated Security** (`Integrated Security=True`) for any domain-joined server — no passwords needed.
- Move non-dev secrets to **environment variables** or a secrets manager (Azure Key Vault, etc.).
- Add `appsettings.Production.json` (without credentials) and document that production connection strings must be injected externally.
- Consider adding `appsettings.json` to `.gitignore` or using `appsettings.Example.json` as a template.

---

### 2. `TrustServerCertificate=True` must not reach production

Even with correct credentials, `TrustServerCertificate=True` silently allows MITM attacks on database traffic. It should only ever appear in a local dev config file that is not deployed.

---

### 3. Internal error messages surfaced directly to the user

[Components/Pages/JvCalculationCheck.razor.cs](Components/Pages/JvCalculationCheck.razor.cs)

```csharp
checkError = string.IsNullOrWhiteSpace(job.ErrorMessage)
    ? "JV background job failed."
    : job.ErrorMessage;
```

`job.ErrorMessage` comes from the database and may contain internal server details (stack traces, query text, internal state). Displaying raw DB-sourced error strings in the UI is an information disclosure risk. Log the full message server-side and show a generic one to the user.

---

## 🟠 Bugs — All Fixed

### 4. `MonitoringOptions` bypassed startup validation ✅ Fixed

[Program.cs](Program.cs) — [Options/MonitoringOptions.cs](Options/MonitoringOptions.cs)

`builder.Services.Configure<MonitoringOptions>` was used instead of `AddOptions<T>().ValidateDataAnnotations().ValidateOnStart()`, meaning a missing or malformed `Monitoring` config section would fail silently at runtime rather than on startup.

**Fix:** Switched to `AddOptions<MonitoringOptions>().Bind(...).ValidateDataAnnotations().ValidateOnStart()` and added `[Required]` / `[Range]` annotations to the options class, consistent with all other options classes.

---

### 5. Hardcoded stored procedure in `MonitoringRepository` bypassed the options pattern ✅ Fixed

[Data/MonitoringRepository.cs](Data/MonitoringRepository.cs) — [Options/MonitoringOptions.cs](Options/MonitoringOptions.cs)

`DbBackupsOverviewStoredProcedure` was a private `const` hardcoded in the repository. All other SP names are configurable via `appsettings.json`.

**Fix:** Added `DbBackupsOverviewStoredProcedure` property to `MonitoringOptions`, removed the constant, and updated `appsettings.json` with the default value.

---

### 6. `ExpireStaleRunningJobsAsync` embedded raw inline SQL instead of a stored procedure ✅ Fixed

[Data/JvCalculationRepository.cs](Data/JvCalculationRepository.cs)

Every other database operation uses stored procedures, but this method contained an inline `UPDATE` statement directly in C# code — hardcoding table name, column names, and business logic.

**Fix:** Added `JobExpireStaleStoredProcedure` to `JvCalculationOptions`, converted the method to `CommandType.StoredProcedure`, and updated `appsettings.json`. The corresponding stored procedure `monitoring.UspJvJobExpireStale` must be deployed to `LOG_FI_ALMT`.

---

### 7. `ExecuteJvJobStateProcedureAsync` detected behavior by comparing the SP name string ✅ Fixed

[Data/JvCalculationRepository.cs](Data/JvCalculationRepository.cs)

```csharp
// Before
if (string.Equals(procedureName, _jvCalculationOptions.JobMarkFailedStoredProcedure, StringComparison.OrdinalIgnoreCase))

// After
if (errorMessage is not null)
```

The decision of whether to add `@ErrorMessage` was based on a string comparison against a config value. If the SP name changed, the parameter would be silently omitted and the SP would fail at runtime.

**Fix:** The condition now uses `errorMessage is not null`, which is already the actual semantic intent — `MarkJvJobFailedAsync` passes a non-null message, while `MarkJvJobCompletedAsync` and `HeartbeatJvJobAsync` pass `null`.

---

### 8. `LoadCobDatesAsync` ignored `disposeCts` when loading dates ✅ Fixed

[Components/Pages/JvCalculationCheck.razor.cs](Components/Pages/JvCalculationCheck.razor.cs)

```csharp
// Before
var response = await Repository.GetJvPnlDatesAsync(CancellationToken.None);

// After
var response = await Repository.GetJvPnlDatesAsync(disposeCts.Token);
```

All other async calls in the component used `disposeCts.Token`. This one did not, so navigating away during the DB call would not cancel it.

---

### 9. Typo in `GetStatusKind` status string ✅ Documented

[Components/Pages/ReplayFlows.razor.cs](Components/Pages/ReplayFlows.razor.cs)

`"submissonstarted"` (misspelling) is retained because it compensates for a typo present in the DB status value. A clarifying comment was added to prevent future "fixes" from breaking the match.

---

### 10. Typo in user-visible error message ✅ Fixed

[Components/Pages/JvCalculationCheck.razor.cs](Components/Pages/JvCalculationCheck.razor.cs)

```csharp
// Before
"Unable to run Fix caculation right now. Please try again."

// After
"Unable to run Fix calculation right now. Please try again."
```

---

### 11. `StatusPollingIntervalSeconds` hardcoded constant ignored the configured option ✅ Fixed

[Components/Pages/ReplayFlows.razor.cs](Components/Pages/ReplayFlows.razor.cs)

`ReplayFlowsOptions` was injected and exposes `StatusPollIntervalSeconds`, but the component used a hardcoded `private const int StatusPollingIntervalSeconds = 15` instead. Changing the config had no effect.

**Fix:** Removed the constant and the timer now reads `ReplayFlowsOptions.Value.StatusPollIntervalSeconds` directly.

---

### 12. `DisplayDateFormat` and `PnlDateDisplayFormat` were identical redundant constants ✅ Fixed

[Components/Pages/ReplayFlows.razor.cs](Components/Pages/ReplayFlows.razor.cs)

Two constants with different names held the exact same value `"dd-MM-yyyy"`. Removed `PnlDateDisplayFormat` and replaced all usages with `DisplayDateFormat`.

---

## 🟡 Design & Maintainability

### 13. Duplicated SQL exception classification logic ✅ Fixed

[Data/JvCalculationRepository.cs](Data/JvCalculationRepository.cs) and [Data/JvCalculationProcessingService.cs](Data/JvCalculationProcessingService.cs)

Both classes contained identical `IsSqlTimeout(SqlException)` and `IsSqlConnectionFailure(SqlException)` static methods.

**Fix:** Moved both methods to `SqlDataHelper`. Both callers now use `SqlDataHelper.IsSqlTimeout(ex)` / `SqlDataHelper.IsSqlConnectionFailure(ex)`. Private copies removed.

---

### 14. `UamAuthorizationRepository` and `UamPermissionHandler` are not `sealed` ✅ Fixed

[Data/UamAuthorizationRepository.cs](Data/UamAuthorizationRepository.cs) — [Security/UamPermissionHandler.cs](Security/UamPermissionHandler.cs)

**Fix:** Added `sealed` to both classes, consistent with all other repositories and services.

---

### 15. Duplicate calendar widget in two page components ✅ Fixed

`ReplayFlows.razor.cs` and `JvCalculationCheck.razor.cs` both implemented nearly identical calendar logic (~80 lines each).

**Fix:** Created `Components/Shared/DatePickerInput.razor` (+ `.razor.cs` + `.razor.css`) as a shared component. Parameters: `Value` (DateOnly?), `ValueChanged` (EventCallback<DateOnly>), `AvailableDates` (IReadOnlyCollection<DateOnly>?), `IsDisabled`, `InputId`, `AriaLabel`. Both pages now use `<DatePickerInput .../>` — all calendar duplication removed.

---

### 16. `GetStatusKind` uses an open-ended set of magic strings for status matching ✅ Fixed

[Components/Pages/ReplayFlows.razor.cs](Components/Pages/ReplayFlows.razor.cs)

**Fix:** Extracted 13 named private constants (`NormalizedCompleted`, `NormalizedInProgress`, `NormalizedSubmissionStartedTypo`, etc.) and updated `GetStatusKind` to use them. The DB typo constant is explicitly documented with a comment.

---

### 17. `SerializeProperties` in `StoredProcedureLogSink` produces an unstructured string ✅ Fixed

[Data/StoredProcedureLogSink.cs](Data/StoredProcedureLogSink.cs)

**Fix:** Replaced the `Key=Value, Key2=Value2` string builder with `JsonSerializer.Serialize(dict)`. Log properties are now stored as structured JSON.

---

### 18. `FormatCellValue` uses `DateTimeStyles.AssumeLocal` for DB-sourced datetimes ✅ Fixed

[Components/Pages/JvCalculationCheck.razor.cs](Components/Pages/JvCalculationCheck.razor.cs)

**Fix:** `FormatCellValue` simplified to `return string.IsNullOrWhiteSpace(value) ? "-" : value`. No datetime parsing — the raw string from `ReadMonitoringTableAsync` is displayed as-is.

---

### 19. JV-related log calls reuse `MonitoringLoadFailed` (event ID 1001) ✅ Fixed

[Components/Pages/JvCalculationCheck.razor.cs](Components/Pages/JvCalculationCheck.razor.cs)

**Fix:** Added `JvPageLoadFailed` (1002) and `JvPageActionFailed` (1003) to `AppLogEvents`. Updated both log call sites in `JvCalculationCheck.razor.cs`.

---

### 20. `UamAuthorizationRepository.IsUserAuthorizedAsync` calls `GetOrdinal` inside a read loop ✅ Fixed

[Data/UamAuthorizationRepository.cs](Data/UamAuthorizationRepository.cs)

**Fix:** Moved `var nameOrdinal = reader.GetOrdinal("Name")` outside the `while` loop. Consistent with every other repository.

---

### 21. `GetReplayFlowProcessStatusAsync` has a misleading name and return type ✅ Fixed

[Data/ReplayFlowRepository.cs](Data/ReplayFlowRepository.cs)

**Fix:** Renamed to `RefreshReplayFlowProcessStatusAsync` in both the repository and its caller in `ReplayFlows.razor.cs`.

---

## 🔵 Minor / Nits

| Location | Issue |
|---|---|
| [Data/MonitoringRepository.cs](Data/MonitoringRepository.cs) | `ReadMonitoringTableAsync` is `internal static` — used cross-class from `JvCalculationRepository`. Worth a comment documenting the intentional coupling. |
| [Components/Pages/ReplayFlows.razor.cs](Components/Pages/ReplayFlows.razor.cs) | `PendingCount`, `InProgressCount`, `CompletedCount` properties are redundant wrappers over identically-named private backing fields. |
| [Components/Pages/JvCalculationCheck.razor.cs](Components/Pages/JvCalculationCheck.razor.cs) | `JobStatusClass` falls back to `jv-status-badge--queued` for unknown statuses, which could silently style a "Failed" job as "Queued" if the string doesn't match. |
| [Data/ReplayFlowProcessingQueue.cs](Data/ReplayFlowProcessingQueue.cs) | `BoundedChannelFullMode.DropWrite` silently discards queue entries when full. This is intentional (all items trigger the same SP), but a comment explaining the design would help. |

---

## Summary

| Severity | Count | Status |
|---|---|---|
| 🔴 Critical / Security | 3 | Open |
| 🟠 Bugs | 9 | ✅ All fixed |
| 🟡 Design | 9 | ✅ All fixed |
| 🔵 Minor | 4 | Open |
