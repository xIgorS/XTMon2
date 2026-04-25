# UspXtgMonitoringBalancesCalculation Optimization Proposal

This file is a review proposal for SQL-team validation. It is not a drop-in replacement script.

## Goal

Reduce request-time freezing and SQL runtime volatility by turning the current monolithic fact validation proc into a staged, index-aware process.

## Main Production Risks In The Current Procedure

1. The procedure is a very large dynamic SQL batch that reads multiple large fact tables, joins current-day and prior-day flow states, joins exchange rates, then performs variance logic in one long execution path.
2. It uses many `SELECT INTO` temp tables, `DISTINCT`, `UNION`, `EXCEPT`, window aggregates, and a `FULL OUTER JOIN`. That is a classic tempdb and memory-grant risk profile.
3. `WITH (NOLOCK)` is used on large fact and flow tables. That can make the monitoring result nondeterministic.
4. There are at least two concrete correctness issues that should be hotfixed before any tuning:
   - `coalesce(arkflowP, 0) < coalesce(arkflowP, 0)` and the same `arkflowS` comparison are always false.
   - `WHERE pnldate = @PnLDate AND IsAdj = 0 OR @IsAdjSplitted = 0` and the matching previous-day clause will broaden the rowset whenever `@IsAdjSplitted = 0`.
5. Fact loads appear to happen before the result set has been reduced enough, so the proc may be pulling more pricing rows than needed.
6. The current architecture recomputes the full validation on demand instead of persisting a job result per `PnLDate` and source system.

## Recommended Handling Order

1. Hotfix correctness bugs first.
2. Remove the monolithic dynamic batch and split the logic into static stages.
3. Reduce the rowset before touching the large fact tables.
4. Add explicit temp-table schemas and indexes instead of relying on `SELECT INTO` heaps.
5. Move execution out of the UI request path if the result can tolerate asynchronous completion.

## Immediate Hotfixes

1. Add parentheses to the `#flowsfinalCurrent` and `#flowsfinalPrev` filters.

```sql
WHERE pnldate = @PnLDate
  AND (IsAdj = 0 OR @IsAdjSplitted = 0)

WHERE pnldate = @PrevPnL
  AND (IsAdj = 0 OR @IsAdjSplitted = 0)
```

2. Fix the always-false `arkflowP` and `arkflowS` comparisons by comparing the intended values, not the same expression to itself.
3. Replace `SELECT * INTO` with explicit column lists for temp tables that are reused downstream.
4. Remove `NOLOCK` from the core flow/fact path if the business result must be consistent.

## Proposed Stage-Oriented Rewrite

### Stage 1. Resolve small reference inputs

Resolve once at the top:

1. previous dates
2. `@IsEOM`, `@SameQuarter`, `@SameMonth`, `@SameYear`
3. business data type IDs
4. measure type IDs
5. feed source IDs

These are cheap and should not be embedded in a dynamic batch.

### Stage 2. Resolve requested source systems

Replace CSV-driven dynamic filtering with parsed temp tables or TVPs.

```sql
CREATE TABLE #RequestedSourceSystems
(
    SourceSystemCode varchar(128) NOT NULL PRIMARY KEY
);

INSERT INTO #RequestedSourceSystems (SourceSystemCode)
SELECT DISTINCT LTRIM(RTRIM(value))
FROM STRING_SPLIT(COALESCE(@SourceSystemCodes, ''), ',')
WHERE LTRIM(RTRIM(value)) <> '';
```

### Stage 3. Create narrow, indexed staging tables

Recommended temp tables:

1. `#FS`
   - key: `(SkSourceData, FeedSourceId)`
2. `#FlowCurrent`
   - key: `(FlowId)`
3. `#FlowPrev`
   - key: `(FlowId)`
4. `#Rate`
   - key: `(SkPortfolio, DateValue, SkCurrency, IsFx, IsFreeze)`
5. `#PricingCurrentAgg`
   - key: `(FlowId, SkSourceData, SkPortfolio, SkMeasureType, SkMeasureSubType, SkSourceCurrency, IsFx)`
6. `#PricingPrevAgg`
   - same key as current

Example pattern:

```sql
CREATE TABLE #FlowCurrent
(
    FlowId bigint NOT NULL PRIMARY KEY CLUSTERED,
    PortfolioFlowIdPriorBalance bigint NULL,
    PnlDate date NOT NULL,
    BusinessDataTypeId smallint NOT NULL,
    WorkflowPnLDailyOpen bit NULL,
    PortfolioId bigint NULL,
    IsAdj bit NOT NULL,
    IsMirrored bit NULL
);
```

### Stage 4. Aggregate facts before large joins

Do not carry the raw fact row shape further than needed.

```sql
CREATE TABLE #PricingCurrentAgg
(
    FlowId bigint NOT NULL,
    SkSourceData int NOT NULL,
    SkPortfolio int NOT NULL,
    SkMeasureType int NOT NULL,
    SkMeasureSubType int NULL,
    SkSourceCurrency int NOT NULL,
    IsFx bit NOT NULL,
    DailyAmountSource decimal(38, 10) NULL,
    ITDAmountSource decimal(38, 10) NULL,
    MTDAmountSource decimal(38, 10) NULL,
    YTDAmountSource decimal(38, 10) NULL,
    PRIMARY KEY CLUSTERED (FlowId, SkSourceData, SkPortfolio, SkMeasureType, SkMeasureSubType, SkSourceCurrency, IsFx)
);

INSERT INTO #PricingCurrentAgg
SELECT
    pnl1.FlowId,
    pnl1.SkSourceData,
    pnl1.SkPortfolio,
    pnl1.SkMeasureType,
    pnl1.SkMeasureSubType,
    pnl1.SkSourceCurrency,
    pnl1.IsFx,
    SUM(pnl1.DailyAmountSource),
    SUM(pnl1.CumulatedITDAmountSource),
    SUM(pnl1.CumulatedMTDAmountSource),
    SUM(pnl1.YTDAmountSource)
FROM dwh.FactPnLPricingAgg pnl1
INNER JOIN #FlowCurrent f1 ON pnl1.FlowId = f1.FlowId
INNER JOIN #FS fs ON fs.SkSourceData = pnl1.SkSourceData
WHERE pnl1.SkPnLTime = @PnLDate
GROUP BY
    pnl1.FlowId,
    pnl1.SkSourceData,
    pnl1.SkPortfolio,
    pnl1.SkMeasureType,
    pnl1.SkMeasureSubType,
    pnl1.SkSourceCurrency,
    pnl1.IsFx;
```

Apply the same pattern to prior-day and adjustment data.

### Stage 5. Replace `FULL OUTER JOIN` if possible

The current `FULL OUTER JOIN` between previous-day and current-day result sets is expensive and often indicates missing key normalization.

Safer pattern:

1. build a unified key table first
2. left join current aggregates
3. left join previous aggregates

```sql
CREATE TABLE #VarianceKeys
(
    PortfolioId bigint NOT NULL,
    SkMeasureType int NOT NULL,
    SkMeasureSubType int NULL,
    SkSourceCurrency int NOT NULL,
    SkSourceData int NOT NULL,
    Typ varchar(32) NOT NULL,
    IsFx bit NOT NULL,
    PRIMARY KEY CLUSTERED (PortfolioId, SkMeasureType, SkMeasureSubType, SkSourceCurrency, SkSourceData, Typ, IsFx)
);
```

### Stage 6. Persist final output instead of recomputing on demand

This procedure is a good candidate for a persisted monitoring-job result table keyed by:

1. `Category`
2. `SubmenuKey` or proc key
3. `PnlDate`
4. source-system scope hash

That would let the app show the latest completed result instead of making the user wait for a very expensive runtime calculation.

## Recommended Supporting Index Review

These should be validated against actual execution plans and existing indexes:

1. `administration.Flows`
   - `(PnlDate, FeedSourceId, BusinessDataTypeId, FeedVersion)`
   - include `FlowId, PortfolioName, FeedSourceDerivedId, WorkflowPnLDailyOpen, TypeOfCalculation, PortfolioFlowIdPriorBalance, PortfolioId`
2. `dwh.FactPnLPricingAgg`
   - `(SkPnLTime, FlowId, SkSourceData)`
3. `dwh.FactPnLPricingAggAdj`
   - `(SkPnLTime, FlowId, SkSourceData)`
4. `dwh.FactPnLSellDownAgg`
   - `(SkPnLTime, FlowId, SkSourceData)`
5. `dwh.FactPnLSellDownAggAdj`
   - `(SkPnLTime, FlowId, SkSourceData)`
6. `dwh.ExchangeRate`
   - `(FromSkCurrency, ToSkCurrency, SkForexSet, DateValue, IsLastVersion)`

## What To Ask The SQL Team To Approve

1. A correctness hotfix release first.
2. A stage-oriented rewrite second.
3. A move from synchronous request execution to asynchronous result generation for this proc if users can tolerate queued completion.
4. Query Store capture and plan comparison before and after the hotfix.

## Expected Outcome If Approved

1. Lower tempdb pressure.
2. Lower risk of oversized memory grants.
3. Less UI freezing because the proc is reduced or moved off the request thread.
4. Easier diagnosis when one stage regresses.