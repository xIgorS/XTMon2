# UspXtgMonitoringBalancesCalculation Execution-Plan Checklist

Use this after deploying the tuned definition to non-prod.

## Preconditions

- Run against a restored or otherwise representative STAGING_FI_ALMT dataset.
- Compare old and new definitions with the same `@PnlDate`, `@SourceSystemCodes`, and `@PrecisionAllowed`.
- If Query Store is enabled, keep it on for both baseline and tuned runs.
- Do not clear the server plan cache unless the DBA explicitly approves it.

## Recommended Test Inputs

Run at least these cases:

1. A normal business day with the widest realistic `@SourceSystemCodes` scope.
2. An end-of-month `@PnlDate`.
3. A narrow source-system scope with only one or two systems.
4. A date known to produce non-empty KO output.

## Capture Script

```sql
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

DECLARE @Query nvarchar(max);

EXEC [monitoring].[UspXtgMonitoringBalancesCalculation]
    @PnlDate = '2026-04-24',
    @Execute = 1,
    @Query = @Query OUTPUT,
    @SourceSystemCodes = 'SYSTEM_A,SYSTEM_B',
    @PrecisionAllowed = 0.01;

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
```

Capture the actual execution plan for both the baseline and tuned versions.

## Functional Equivalence Checks

- Row count from the final result set is unchanged for the same inputs.
- The set of `Status`, `Typ`, `SourceDataName`, `PortfolioName`, `MeasureTypeName`, `SubMeasureTypeName`, `CurrencyCode`, and `FlowIdPreviousBalance` values is unchanged.
- If you store outputs into temp tables for comparison, verify no row-level differences other than ordering.

## Plan Checks

Review the actual plan for these points:

1. The joins from `#tmpBFEDaytmp` and `#tmpBFELASTDaytmp` into `#rate` should use the new ordered temp structures instead of two unmanaged heaps.
2. The `#tmpBFEDay` to `#tmpBFELASTDay` variance stage should show lower spill risk; ideally the join no longer spills to tempdb.
3. The memory grant for the variance section should be lower or at least more stable across repeated runs.
4. Worktable and workfile spills should be reduced or eliminated in the hottest part of the plan.
5. The `#FS` lookups by `FeedSourceId` should be able to use `IX_FS_FeedSourceId`.

## Metrics To Record

For each run, capture:

- elapsed time
- CPU time
- logical reads per major table
- tempdb spill warnings
- granted memory and used memory
- final row count

## Success Criteria

Treat the tuning pass as acceptable when all of these hold:

1. Output is functionally equivalent for the chosen test inputs.
2. No new tempdb spills are introduced.
3. The variance stage is at least as fast as baseline and preferably materially faster.
4. CPU and logical reads do not regress on the narrow-scope case.
5. The end-of-month case does not show a worse memory-grant profile than baseline.

## Follow-Up If The Plan Is Still Heavy

- Check whether `#tmpBFEDay` and `#tmpBFELASTDay` still contain duplicate join keys that force a large hash full join.
- Inspect whether the rate join remains a dominant cost despite the new temp indexes.
- Consider a deeper rewrite of the variance stage into a keyed left-join plus anti-join pattern only after validating that duplicate-key semantics are preserved.
