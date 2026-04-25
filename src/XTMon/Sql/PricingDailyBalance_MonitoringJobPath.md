# Pricing And Daily Balance Monitoring-Job Path

## Verified Flow

Pricing and Daily Balance already run asynchronously for user-triggered executions.

1. The page `RunAsync` method enqueues a monitoring job instead of calling the heavy stored procedure directly.
2. The page stores the returned job id and polls lightweight job state until the job reaches a terminal status.
3. The heavy stored procedure runs inside the background monitoring-job executor, not inside the Blazor UI event handler.
4. The worker sends heartbeats while the job is running, saves the final result payload, and marks the job completed or failed.
5. The page restores the latest saved result by key when the page loads or when the selected PnL date changes.

## What "Move Fully Onto The Monitoring-Job Path So The UI Never Waits" Means Here

For this codebase, it does not mean adding queueing from scratch. That path already exists.

It means keeping the UI on the lightweight path only:

1. enqueue work
2. poll job state
3. render saved result

The UI should never directly await the long-running Pricing or Daily Balance stored procedure call.

## Verified Decision

No direct synchronous heavy-procedure execution path was found for the Pricing or Daily Balance Run actions.

The remaining app-side bottleneck is the shared `DataValidation` monitoring-job processor lane:

1. Pricing and Daily Balance use the same `DataValidation` category as the rest of the data-validation checks.
2. The monitoring job claim procedure is FIFO by enqueue time.
3. The shared Data Validation processor can therefore spend its worker slots on these heavy jobs while other Data Validation work waits.

## Implemented Change

The implementation keeps Pricing and Daily Balance in the existing `DataValidation` category so that:

1. page restore-latest behavior stays unchanged
2. Data Validation runner aggregation stays unchanged
3. sidebar/nav alert aggregation stays unchanged

Instead of changing categories, the monitoring-job claim path was extended to support submenu-level inclusion and exclusion.

That enables three processors to coexist safely:

1. the shared Data Validation processor now excludes `pricing` and `daily-balance`
2. a dedicated Pricing processor claims only `pricing`
3. a dedicated Daily Balance processor claims only `daily-balance`

This is an app-side isolation change, not a rewrite of the heavy SQL procedures.

## What Remains A SQL-Team Concern

This change reduces queue contention and gives better operational control, but it does not fix SQL saturation caused by the procedures themselves.

The SQL-team concerns remain:

1. plan instability
2. tempdb pressure
3. memory grants
4. `NOLOCK` correctness risk
5. dynamic SQL filter shape
6. heavy fact-table processing

Those concerns are documented in:

1. `UspXtgMonitoringPricingDaily.proposal.md`
2. `UspXtgMonitoringBalancesCalculation.proposal.md`

## Expected Outcome

1. Pricing and Daily Balance no longer consume slots from the shared Data Validation worker.
2. The UI still only enqueues and polls.
3. Data Validation status and latest-result behavior remain stable.
4. If freezes continue after this change, the next root cause is more likely SQL-server pressure than app-side orchestration.