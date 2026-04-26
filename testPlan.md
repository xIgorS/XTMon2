# Playwright End-to-End Test Plan

## 1. Objective

Build a full browser-driven Playwright test suite for XTMon that covers:

1. Every routable page in `src/XTMon/Components/Pages`.
2. Every user-clickable button and button-like action surfaced by those pages and the shared layout.
3. Every hosted service and user-visible service workflow that the browser can trigger directly or observe indirectly.
4. Every critical backend side effect that must be validated with DB or diagnostics assertions in addition to browser checks.

The suite must be strong enough to catch regressions in:

1. Navigation and shared layout behavior.
2. Authentication and authorization gates.
3. Global PNL date propagation.
4. Monitoring job orchestration and category-specific processors.
5. Replay flows and JV calculation background processing.
6. System diagnostics, cleanup, cancellation, and recovery actions.
7. Service-driven UI state such as nav alert badges and persisted job restore behavior.

## 2. Scope

### In scope

1. All 40 routable pages found in `src/XTMon/Components/Pages/*.razor`.
2. Shared layout and navigation surfaces in the sidebar and reconnect modal.
3. All visible buttons, icon buttons, row actions, toggle buttons, and button-like links that behave as primary actions.
4. Browser-observable hosted/background flows backed by SQL Server and Blazor Server polling.
5. Service health and data correctness assertions that require DB validation after browser actions.

### Out of scope for browser-only validation

1. Pure helper logic already covered by unit tests.
2. Silent internal exception classification that has no UI effect.
3. SQL-only edge cases that cannot be deterministically triggered through the browser without controlled DB setup.

These out-of-scope items still need coverage through unit tests, integration tests, or DB/log assertions attached to the Playwright run.

## 3. Test Environment

### Runtime

1. Run XTMon in Development on `https://localhost:7009`.
2. Use a Windows-authenticated browser session because the app uses Negotiate/NTLM.
3. Primary browser target: Chromium / Edge channel.
4. Optional smoke expansion: Firefox or WebKit only if Windows authentication and the local environment support them.

### Required users

1. Authorized user with access to UAM-restricted pages.
2. Unauthorized or non-UAM user for access-denied coverage.
3. Anonymous or unauthenticated scenario only if the environment permits simulating it.

### Required data and DB state

1. Known valid PNL date in the sidebar and in page-level pickers.
2. Replay flow rows available for at least one PNL date.
3. JV calculation COB dates available.
4. Monitoring job tables writable and observable.
5. Clean baseline with no leftover simulation harnesses enabled.
6. Known cleanup strategy for any destructive job execution done by the suite.

### Required observability

1. Browser assertions.
2. SQL assertions against relevant databases.
3. Application log inspection where UI alone cannot prove the behavior.
4. Screenshots, traces, and video on failure.

## 4. Playwright Architecture

### Recommended structure

1. `playwright.config.ts`
2. `tests/e2e/navigation.spec.ts`
3. `tests/e2e/home.spec.ts`
4. `tests/e2e/replay-flows.spec.ts`
5. `tests/e2e/jv-calculation.spec.ts`
6. `tests/e2e/data-validation-runner.spec.ts`
7. `tests/e2e/functional-rejection-runner.spec.ts`
8. `tests/e2e/system-diagnostics.spec.ts`
9. `tests/e2e/application-logs.spec.ts`
10. `tests/e2e/monitoring-smoke.spec.ts`
11. `tests/e2e/monitoring-job-pages.spec.ts`
12. `tests/e2e/authz.spec.ts`
13. `tests/e2e/reconnect-modal.spec.ts`
14. `tests/e2e/not-found-error.spec.ts`
15. `tests/e2e/button-audit.spec.ts`
16. `tests/helpers/dbAssertions.ts`
17. `tests/helpers/nav.ts`
18. `tests/helpers/pnlDate.ts`
19. `tests/helpers/jobPolling.ts`
20. `tests/helpers/buttonAudit.ts`

### Recommended fixtures

1. `authorizedPage`
2. `unauthorizedPage`
3. `dbClient`
4. `testPnlDate`
5. `jobCleanup`
6. `consoleErrorCollector`
7. `networkFailureHarness`

### Standard assertion rules for every spec

1. No uncaught page exceptions.
2. No unexpected browser console errors.
3. No unhandled 500 responses for the covered happy path.
4. Expected route title and page heading must render.
5. Primary actions must show correct enabled and disabled transitions.
6. Every destructive action must leave the environment in a known state or run against isolated data.

## 5. Mandatory Button Coverage Rule

To satisfy "test every button", the suite must include an automated button census and action audit.

### Button census workflow

For each route:

1. Load the page in its default state.
2. Capture all visible `button` elements using Playwright role locators.
3. Open every secondary state that reveals more buttons.
4. Recapture buttons after each reveal step.
5. Compare the discovered set to an expected registry for that route.
6. Fail the test if a new visible button appears without an associated test case.

### Secondary states that must be opened before button capture

1. Expanded navigation groups in the sidebar.
2. Query panels on monitoring pages.
3. Status-detail toggles on JV Calculation.
4. Submitted-flow status section on Replay Flows.
5. Stuck-jobs and processor-health sections on System Diagnostics.
6. Active-job states that expose `Cancel` buttons.
7. Reconnect modal.
8. Table row expansion on Application Logs.

### Button audit assertions per button

Each discovered button must have coverage for:

1. Label correctness.
2. Initial enabled or disabled state.
3. State transition rules when prerequisites are missing.
4. Click behavior.
5. Side effect in UI, DB, or logs.
6. Error message or disabled-state behavior when the action is intentionally unavailable.

## 6. Coverage Profiles

Use the following shared coverage profiles to avoid duplicating logic while still covering every page.

| Profile | Applies to | Button family | Required assertions |
|---|---|---|---|
| `Shell` | Home and global layout | Theme toggle, nav links, date picker interactions | Route load, nav state, date propagation, layout persistence |
| `DashboardRefresh` | Monitoring dashboards | `Refresh data` | Refresh works, last refresh updates, data renders |
| `ReplayFlows` | Replay Flows | `Refresh`, `Select All`, `Submit (n)`, `Check processing status` | Input validation, load, submit, poll, row selection, status updates |
| `JvCalculation` | JV Calculation Check | `Reload COB dates`, `Run Check`, `Cancel Job`, `Hide/Show Check Query`, `Copy SQL to Clipboard`, `Hide/Show Fix Query`, `Fix Calculation` | Date loading, job execution, query reveal, copy, cancellation, result rendering |
| `Runner` | Data Validation Runner, Functional Rejection Runner | `Refresh`, `Select All`, `Clear`, `Run Selected (n)`, per-row `Cancel` | Selection rules, batch submit, polling, row cancellation, nav badge updates |
| `MonitoringJobBase` | Batch Status and Functional Rejection detail | Run action, `Retry`, `Resume`, active-state `Cancel` if present | Restore latest result, run flow, polling, retry/resume behavior |
| `MonitoringTable` | Most monitoring-job pages | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Query visibility, result grid, retry/resume, saved state restore |
| `SourceSystemMonitoringTable` | Pricing, Daily Balance, Adjustments, Reverse Conso File | `Reload source systems`, source-system `Select all`, run action, query toggle, retry, resume | Source system loading, selection persistence, run flow |
| `Diagnostics` | System Diagnostics | `Cancel All Background Jobs`, `Run/Re-run Diagnostics`, `Open Application Logs`, `Reset Orphaned Statuses`, `Load/Refresh Stuck Jobs`, `Force-Expire All Stuck Jobs`, `Clean Logging`, `Clean History`, `Load/Refresh Processor Health` | Report correctness, cleanup, cancellation, recovery, processor health, stuck jobs |
| `ApplicationLogs` | Application Logs | `Refresh`, `Reset Filters`, row expand or collapse | Filter application, refresh, row detail expansion |
| `PassiveError` | Error and Not Found | No primary actions expected | Correct passive render and safe navigation away |
| `ReconnectModal` | Global reconnect surface | `Retry`, `Resume` | Modal visibility, reconnect behavior, resume behavior |

## 7. Full Route Coverage Matrix

Every route must have at least one dedicated Playwright test and be part of the button census.

| Route | Page | Coverage profile | Key buttons and actions | Backend and service coverage |
|---|---|---|---|---|
| `/` | Home | `Shell` | Theme toggle, nav links, quick navigation cards, global PNL date | `PnlDateState`, nav alert states |
| `/monitoring` | Monitoring | `DashboardRefresh` | `Refresh data` | Monitoring repository, DB space rendering |
| `/db-backup-info` | DB Backup Info | `DashboardRefresh` | `Refresh data` | Backup repository, latest backup data |
| `/replay-flows` | Replay Flows | `ReplayFlows` | `Refresh`, `Select All`, `Submit (n)`, `Check processing status`, row checkboxes, `Skip Core`, `Drop Tmp` | `ReplayFlowProcessingQueue`, `ReplayFlowProcessingService`, replay repositories |
| `/jv-calculation-check` | JV Calculation Check | `JvCalculation` | `Reload COB dates`, `Run Check`, `Cancel Job`, status badge toggle, `Show/Hide Check Query`, `Copy SQL to Clipboard`, `Show/Hide Fix Query`, `Fix Calculation` | `JvCalculationProcessingService`, JV repository, nav alert state |
| `/system-diagnostics` | System Diagnostics | `Diagnostics` | `Cancel All Background Jobs`, `Run Diagnostics` or `Re-run Diagnostics`, `Open Application Logs`, `Reset Orphaned Statuses`, `Load Stuck Jobs`, `Force-Expire All Stuck Jobs`, `Clean Logging`, `Clean History`, `Load Processor Health` or `Refresh Processor Health` | `DeploymentCheckService`, `JobDiagnosticsService`, `BackgroundJobCancellationService`, startup recovery |
| `/application-logs` | Application Logs | `ApplicationLogs` | `Refresh`, `Reset Filters`, row expand | Application logs repository, DB log viewer |
| `/data-validation-runner` | Data Validation Runner | `Runner` | `Refresh`, `Select All`, `Clear`, `Run Selected (n)`, per-row `Cancel` | Monitoring job orchestration, DV nav alert state, category processor limits |
| `/functional-rejection-runner` | Functional Rejection Runner | `Runner` | `Refresh`, `Select All`, `Clear`, `Run Selected (n)`, per-row `Cancel` | Functional rejection processor, menu state, nav alert state |
| `/batch-status` | Batch Status | `MonitoringJobBase` | Run action, `Retry`, `Resume`, active-state `Cancel` if present | `BatchStatusMonitoringJobExecutor`, monitoring job orchestration |
| `/functional-rejection` | Functional Rejection detail | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume`, active-state `Cancel` if present | `FunctionalRejectionMonitoringJobExecutor`, menu state, monitoring job orchestration |
| `/pricing` | Pricing | `SourceSystemMonitoringTable` | `Reload source systems`, source-system `Select all`, `Run Pricing`, `Show Query`, `Hide Query`, `Retry`, `Resume` | `PricingMonitoringJobProcessingService`, `DataValidationMonitoringJobExecutor`, source-system repository |
| `/daily-balance` | Daily Balance | `SourceSystemMonitoringTable` | `Reload source systems`, source-system `Select all`, `Run Daily Balance`, `Show Query`, `Hide Query`, `Retry`, `Resume` | `DailyBalanceMonitoringJobProcessingService`, monitoring executor |
| `/adjustments` | Adjustments | `SourceSystemMonitoringTable` | `Reload source systems`, source-system `Select all`, run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor, source-system repository |
| `/reverse-conso-file` | Reverse Conso File | `SourceSystemMonitoringTable` | `Reload source systems`, source-system `Select all`, run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor, source-system repository |
| `/referential-data` | Referential Data | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/market-data` | Market Data | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor, DTM connection path |
| `/pricing-file-reception` | Pricing File Reception | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/out-of-scope-portfolio` | Out of Scope Portfolio | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/missing-sog-check` | Missing SOG Check | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/adjustment-links-check` | Adjustment Links Check | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/column-store-check` | Column Store Check | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/trading-vs-fivr-check` | Trading vs FIVR Check | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/mirrorization` | Mirrorization | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/result-transfer` | Result Transfer | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/rollovered-portfolios` | Rollovered Portfolios | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/sas-tables` | SAS Tables | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/non-xtg-portfolio` | Non-XTG Portfolio | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/rejected-xtg-portfolio` | Rejected XTG Portfolio | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/feedout-extraction` | FeedOut Extraction | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/future-cash` | Future Cash | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/fact-pv-ca-consistency` | Fact PV/CA Consistency | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/multiple-feed-version` | Multiple Feed Version | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/publication-consistency` | Publication Consistency | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor, publication DB path |
| `/jv-balance-consistency` | JV Balance Consistency | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor, publication DB path |
| `/missing-workflow-check` | Missing Workflow Check | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/precalc-monitoring` | Precalc Monitoring | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/vrdb-status` | VRDB Status | `MonitoringTable` | Run action, `Show Query`, `Hide Query`, `Retry`, `Resume` | Data validation executor |
| `/Error` | Error | `PassiveError` | No primary button requirement; verify passive render and navigation recovery | Error handling surface |
| `/not-found` | Not Found | `PassiveError` | No primary button requirement; verify passive render and navigation recovery | 404 route surface |

## 8. Shared Layout and Navigation Coverage

These surfaces must be exercised from multiple pages, not only from the Home page.

| Surface | What to test | Services or state covered |
|---|---|---|
| Sidebar nav links | Every link navigates to the correct route | Routing, layout rendering |
| Collapsible menu groups | Expand and collapse Data Validation and Functional Rejection groups | Nav menu logic |
| Active route highlight | Correct group and leaf item remain active after navigation | Nav menu state |
| Global PNL date picker | Changing the date updates all dependent pages and runner statuses | `PnlDateState` |
| Nav alert badges | Replay, JV, Data Validation, Functional Rejection badges appear and clear at the right times | Nav alert state services |
| Theme toggle | Toggle theme on at least one page and confirm persistence within session | Layout state |
| Diagnostics OK indicator | Present after successful diagnostics state load | `StartupDiagnosticsState`, diagnostics wiring |
| Reconnect modal | Simulate circuit interruption, then click `Retry` and `Resume` | Reconnect modal JS, Blazor resume flow |

## 9. Service Coverage Matrix

Every real service in `src/XTMon/Services` that drives browser-visible behavior must be covered here.

| Service | Type | Browser entry point | Required E2E proof |
|---|---|---|---|
| `StartupJobRecoveryService` | Hosted service | App startup and `Reset Orphaned Statuses` | Startup does not leave stale running rows; manual recovery button works |
| `ReplayFlowProcessingService` | Hosted service | Replay Flows submit flow | Status progresses from submitted to processed |
| `ReplayFlowProcessingQueue` | Queue | Replay Flows submit flow | Submitted rows appear and eventually drain |
| `JvCalculationProcessingService` | Hosted service | JV Calculation page | Job state, result grid, cancellation |
| `MonitoringJobProcessingService` | Hosted base processor | All monitoring-job pages | Jobs move through queued, running, completed, failed, cancelled |
| `DataValidationMonitoringJobProcessingService` | Hosted specialized processor | Data Validation Runner and DV pages | DV processor limit and polling behavior |
| `FunctionalRejectionMonitoringJobProcessingService` | Hosted specialized processor | Functional Rejection Runner and detail page | FR jobs execute and poll correctly |
| `PricingMonitoringJobProcessingService` | Hosted specialized processor | Pricing page and DV runner overlap tests | Dedicated pricing lane is used |
| `DailyBalanceMonitoringJobProcessingService` | Hosted specialized processor | Daily Balance page and DV runner overlap tests | Dedicated daily balance lane is used |
| `BatchStatusMonitoringJobExecutor` | Monitoring executor | Batch Status page or runner | Result persisted and displayed |
| `DataValidationMonitoringJobExecutor` | Monitoring executor | All DV monitoring pages | Result payload saved and restored |
| `FunctionalRejectionMonitoringJobExecutor` | Monitoring executor | FR runner and detail page | Result payload saved and rendered |
| `BackgroundJobCancellationService` | Application service | `Cancel All Background Jobs` | All active jobs cancelled and DB state cleared |
| `JobCancellationRegistry` | Coordination service | Per-row `Cancel`, page `Cancel Job`, global cancel | Cancellation is observed by active workers |
| `DeploymentCheckService` | Singleton service | `Run Diagnostics` or `Re-run Diagnostics` | DB and stored proc report matches live configuration |
| `JobDiagnosticsService` | Singleton service | `Load Stuck Jobs`, `Force-Expire All Stuck Jobs`, `Load Processor Health` | Report rows match DB state and cleanup works |
| `PnlDateState` | Scoped UI state | Sidebar date picker and all dependent pages | Selected date propagates consistently |
| `DataValidationNavAlertState` | Scoped UI state | DV runner and sidebar | Badge appears while jobs active and clears afterward |
| `FunctionalRejectionNavAlertState` | Scoped UI state | FR runner and sidebar | Badge appears while jobs active and clears afterward |
| `JvCalculationNavAlertState` | Scoped UI state | JV page and sidebar | Badge appears while job active and clears afterward |
| `ReplayFlowsNavAlertState` | Scoped UI state | Replay Flows and sidebar | Badge appears while relevant work is active |
| `DatabaseSpaceNavAlertState` | Scoped UI state | Monitoring overview if surfaced in nav or cards | Status consistency if wired to UI |
| `FunctionalRejectionMenuState` | Scoped UI/menu state | FR nav, runner, detail page | Menu items load, refresh, and match DB-driven submenu data |
| `StartupDiagnosticsState` | Scoped or singleton state | Layout diagnostics indicator | Layout shows correct diagnostics status |

## 10. Non-Browser Assertions Required Alongside Playwright

Some service behaviors cannot be proven by browser state alone.

| Scenario | Browser assertion | Additional proof |
|---|---|---|
| Replay flow submission | Submitted rows and status grid update | Query replay batch tables for expected rows |
| JV result persistence | Result grid and status change | Query JV job tables for saved result |
| Monitoring job save result | Query panel and result grid render | Query monitoring jobs and latest result payloads |
| Processor concurrency | UI shows running states | Query running and queued monitoring job counts |
| Dedicated pricing and daily balance lanes | Pricing and Daily Balance show active state | Query running jobs by submenu and category |
| Stuck-job expiration | Diagnostics summary updates | Query failed job rows and relevant log entries |
| Cancel-all behavior | UI summary says no active jobs remain | Query active monitoring and JV job counts |
| Startup recovery | UI loads and recovery summary appears | Query previously stale rows before and after recovery |
| UAM authorization | Restricted route blocks unauthorized access | Confirm DB-driven UAM result if test harness supports it |
| Application logs viewer | Browser shows filtered rows | Query `LOG_FI_ALMT` to confirm filter correctness |

## 11. Detailed Test Scenarios by Area

### A. Navigation and shell

1. Load Home page.
2. Validate theme toggle.
3. Navigate through every sidebar leaf route.
4. Validate the active nav item and expanded parent section on each route.
5. Change global PNL date and confirm at least one DV page, one runner, one replay page, and one diagnostics page observe the new date.
6. Validate the reconnect modal using a forced circuit interruption.

### B. Authorization

1. Open each UAM-restricted route with an authorized user.
2. Open the same routes with an unauthorized user.
3. Verify allowed access for the authorized user.
4. Verify redirect, denial, or safe failure for the unauthorized user.

Restricted routes to cover:

1. `/replay-flows`
2. `/jv-calculation-check`
3. `/system-diagnostics`
4. `/application-logs`
5. `/batch-status`

### C. Replay Flows

1. Enter an invalid replay flow set and verify validation errors.
2. Enter a valid replay flow set and click `Refresh`.
3. Apply feed source and calculation type filters.
4. Click `Select All`.
5. Toggle row checkboxes and row-level options.
6. Click `Submit (n)`.
7. Verify submitted rows appear.
8. Click `Check processing status`.
9. Verify pending, in-progress, and completed counters.
10. Confirm DB-side submission rows and processing state.

### D. JV Calculation

1. Click `Reload COB dates`.
2. Pick a valid date.
3. Click `Run Check`.
4. Toggle the job status badge details.
5. Click `Show Check Query` and `Copy SQL to Clipboard`.
6. If fix SQL exists, click `Show Fix Query` and `Copy SQL to Clipboard`.
7. If rows exist, click `Fix Calculation`.
8. If a job is active, click `Cancel Job`.
9. Validate DB-side job rows and status transitions.

### E. Data Validation Runner

1. Load the page with a valid PNL date.
2. Click `Refresh`.
3. Click `Select All`.
4. Click `Clear`.
5. Re-select rows and click `Run Selected (n)`.
6. Validate row status transitions.
7. Click row-level `Cancel` for an active row.
8. Verify the Data Validation nav badge changes while jobs run.
9. Validate DB-side job rows and processor counts.

### F. Functional Rejection Runner

1. Load the runner and refresh statuses.
2. Click `Select All`.
3. Click `Clear`.
4. Re-select rows and click `Run Selected (n)`.
5. Validate row status transitions.
6. Click row-level `Cancel` for an active row.
7. Validate the Functional Rejection nav badge.
8. Validate DB-side FR job rows.

### G. Monitoring job pages

For every route using `MonitoringJobPageBase`, `MonitoringTableJobPageBase`, or `SourceSystemMonitoringTableJobPageBase`:

1. Load the page.
2. If source systems apply, click `Reload source systems` and use source-system selection controls.
3. Run the page-specific action.
4. Verify the page enters active or running state.
5. Verify saved results restore on reload.
6. Click `Show Query`, then `Hide Query`.
7. If active state exposes cancellation, click `Cancel`.
8. Use `Retry` and `Resume` when the page state makes them available.

This group includes:

1. `/batch-status`
2. `/functional-rejection`
3. `/pricing`
4. `/daily-balance`
5. `/adjustments`
6. `/reverse-conso-file`
7. `/referential-data`
8. `/market-data`
9. `/pricing-file-reception`
10. `/out-of-scope-portfolio`
11. `/missing-sog-check`
12. `/adjustment-links-check`
13. `/column-store-check`
14. `/trading-vs-fivr-check`
15. `/mirrorization`
16. `/result-transfer`
17. `/rollovered-portfolios`
18. `/sas-tables`
19. `/non-xtg-portfolio`
20. `/rejected-xtg-portfolio`
21. `/feedout-extraction`
22. `/future-cash`
23. `/fact-pv-ca-consistency`
24. `/multiple-feed-version`
25. `/publication-consistency`
26. `/jv-balance-consistency`
27. `/missing-workflow-check`
28. `/precalc-monitoring`
29. `/vrdb-status`

### H. System Diagnostics

1. Click `Run Diagnostics` or `Re-run Diagnostics`.
2. Verify each database and stored procedure report renders.
3. Click `Open Application Logs`.
4. Click `Reset Orphaned Statuses`.
5. Click `Load Stuck Jobs`, then `Refresh Stuck Jobs`.
6. Click `Force-Expire All Stuck Jobs` in a controlled setup.
7. If cleanup buttons are enabled, click `Clean Logging` and `Clean History` only in an isolated test database or seeded sandbox.
8. Click `Load Processor Health` and `Refresh Processor Health`.
9. Click `Cancel All Background Jobs` while jobs are active.
10. Confirm browser summary and DB state agree.

### I. Application Logs

1. Open the logs page from diagnostics and directly by route.
2. Set filters for top-N, levels, timespan, and message text.
3. Click `Refresh`.
4. Click `Reset Filters`.
5. Expand and collapse a row.
6. Verify filtered browser rows match DB query expectations.

### J. Error and Not Found

1. Open `/not-found` and confirm the passive error view renders safely.
2. Force or simulate the `/Error` route if supported by the environment.
3. Verify safe navigation away from passive error pages.

## 12. Service-to-Page Dependency Matrix

This matrix ensures every service is exercised through at least one browser flow.

| Service family | Primary pages |
|---|---|
| Replay processing | `/replay-flows` |
| JV processing | `/jv-calculation-check` |
| Monitoring processors | `/data-validation-runner`, `/functional-rejection-runner`, `/batch-status`, all monitoring job pages |
| Pricing dedicated processor | `/pricing`, `/data-validation-runner` |
| Daily balance dedicated processor | `/daily-balance`, `/data-validation-runner` |
| Functional rejection menu and processor | `/functional-rejection-runner`, `/functional-rejection` |
| Diagnostics and cleanup | `/system-diagnostics` |
| Application log access | `/application-logs`, `/system-diagnostics` |
| Nav alert states | Any page that can launch jobs plus the sidebar |
| Global PNL date state | Sidebar plus all date-aware pages |

## 13. Execution Phases

### Phase 1: Smoke

Run on every PR.

1. Home and navigation.
2. Monitoring and DB Backup dashboards.
3. Replay Flows load-only path.
4. JV Calculation load-only path.
5. Data Validation Runner load and selection.
6. Functional Rejection Runner load and selection.
7. System Diagnostics render and run diagnostics.
8. Application Logs render and refresh.

### Phase 2: Full regression

Run before merge to protected branches or before release.

1. Every route in the coverage matrix.
2. Button census for every route.
3. Job submission, polling, cancellation, retry, and resume flows.
4. Nav badge transitions.
5. DB-side assertions for orchestration tables.

### Phase 3: Destructive and maintenance

Run only in isolated environments.

1. `Cancel All Background Jobs`
2. `Force-Expire All Stuck Jobs`
3. `Clean Logging`
4. `Clean History`
5. Recovery actions
6. Replay and monitoring submissions against disposable data

### Phase 4: Resilience

Run nightly or on demand.

1. Circuit interruption and reconnect modal.
2. Concurrent jobs across Data Validation, Pricing, Daily Balance, and Functional Rejection.
3. Long-running polling stability.
4. Unauthorized-user access matrix.

## 14. Reporting and Artifacts

Each run must publish:

1. HTML Playwright report.
2. Trace on failure.
3. Screenshot on failure.
4. Video on failure for destructive or flaky flows.
5. Route-to-button inventory artifact.
6. DB assertion summary per test file.
7. Service coverage checklist marking which services were exercised.

## 15. Exit Criteria

The plan is satisfied only when:

1. Every route in the 40-route matrix has at least one Playwright test.
2. Every visible button discovered by the button census has an associated assertion and click-path test.
3. Every service in the service coverage matrix is exercised by at least one browser flow.
4. Every destructive action has a cleanup step.
5. Every browser-only test that needs DB proof has a companion DB assertion.
6. UAM-restricted routes are tested with both allowed and denied access where the environment supports it.
7. Reconnect behavior, nav badges, and global PNL date propagation are covered.
8. The suite can run in smoke mode and full-regression mode.

## 16. Immediate Implementation Order

Build the suite in this order:

1. Navigation and shell.
2. Diagnostics and Application Logs.
3. Replay Flows.
4. JV Calculation.
5. Data Validation Runner.
6. Functional Rejection Runner.
7. Monitoring dashboard pages.
8. Monitoring job pages by shared profile.
9. Unauthorized-access and reconnect scenarios.
10. Button census and service coverage reporting.
