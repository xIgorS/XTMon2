# UspXtgMonitoringPricingDaily Optimization Proposal

This file is a review proposal for SQL-team validation. It is not a drop-in replacement script.

## Goal

Keep the current business output shape, but reduce plan instability, incorrect monitoring results, and long-running request execution.

## Main Production Risks In The Current Procedure

1. The procedure is assembled as a large dynamic SQL string, but only a small subset of values is actually parameterized.
2. `@SourceSystemCodes` and `@BookNames` are injected into `IN (...)` predicates as string text, which gives weak cardinality estimates and poor plan reuse.
3. `WITH (NOLOCK)` is used across status, feed-source, dimension, and flow reads. That can produce false `MISSING`, false `KO`, duplicate rows, and inconsistent joins.
4. Several `SELECT DISTINCT` loads are compensating for row explosion instead of fixing the join shape.
5. Scalar functions such as `fn_DayIsEndOfMonth`, `fn_GetPreviousBusinessDate_N`, and `fn_IsWeek` are used inside row-by-row classification logic even though they only depend on the input date.
6. The `#Scope` load depends on `#SATE`, which looks suspicious and should be validated against the actual deployed proc.
7. The current design falls back to loading the full source-system or book scope when the filtered load returns no rows. That is useful functionally, but it also creates very different runtime profiles for similar inputs.

## Recommended Handling Order

1. Pull the exact deployed proc text from `sys.sql_modules` and confirm whether `#SATE` is real or a bad export.
2. Keep the same procedure contract first. Do not combine a logic rewrite with an interface change.
3. Replace the dynamic `IN (...)` filter construction with static SQL plus parsed filter tables.
4. Remove `NOLOCK` from decision-making paths. Prefer database-level `READ COMMITTED SNAPSHOT` if blocking is the reason those hints were added.
5. Stage intermediate sets in explicit temp tables with targeted indexes instead of repeated `SELECT DISTINCT` over wide joins.
6. Precompute all date flags once at the top of the proc.

## Lowest-Risk Hotfixes

1. Validate and fix the `#SATE` reference.
2. Precompute these once into variables:
   - `@IsEndOfMonth`
   - `@IsSpecialMonthEnd`
   - `@IsWeek`
   - `@LastEOM`
3. Stop reading status IDs via multiple singleton selects. Load them once from one statement or a small temp table.
4. Put explicit indexes on the staging tables that drive the big joins:
   - `#FS (FeedSourceId)`
   - `#Books (SkNoBook, SkMappingGroup)`
   - `#SignOffGroups (SkMappingGroup)`
   - `#Scope (PortfolioName, BusinessDataTypeId, FeedSourceId, PnlDate, TypeOfCalculation)`
5. If parameter skew is still severe after static filters, use `OPTION (RECOMPILE)` only on the final classification query instead of generating the entire procedure dynamically.

## Proposed Static-SQL Shape

### 1. Parse filters once

Preferred option if the caller can change: table-valued parameters.

Fallback option if the caller cannot change: `STRING_SPLIT` into temp tables.

```sql
CREATE TABLE #RequestedSourceSystems
(
    SourceSystemCode varchar(128) NOT NULL PRIMARY KEY
);

INSERT INTO #RequestedSourceSystems (SourceSystemCode)
SELECT DISTINCT LTRIM(RTRIM(value))
FROM STRING_SPLIT(COALESCE(@SourceSystemCodes, ''), ',')
WHERE LTRIM(RTRIM(value)) <> '';

CREATE TABLE #RequestedBooks
(
    BookName nvarchar(256) NOT NULL PRIMARY KEY
);

INSERT INTO #RequestedBooks (BookName)
SELECT DISTINCT LTRIM(RTRIM(value))
FROM STRING_SPLIT(COALESCE(@BookNames, ''), ',')
WHERE LTRIM(RTRIM(value)) <> '';
```

### 2. Resolve reference values once

```sql
SELECT
    @NOT_START = MAX(CASE WHEN StatusCode = 'NOT_START' THEN StatusId END),
    @COMP = MAX(CASE WHEN StatusCode = 'COMP' THEN StatusId END),
    @FAIL = MAX(CASE WHEN StatusCode = 'FAIL' THEN StatusId END),
    @COMP_ERR = MAX(CASE WHEN StatusCode = 'COMP_ERR' THEN StatusId END),
    @PGRS = MAX(CASE WHEN StatusCode = 'PGRS' THEN StatusId END),
    @CANCEL = MAX(CASE WHEN StatusCode = 'CANCEL' THEN StatusId END),
    @DELAY = MAX(CASE WHEN StatusCode = 'DELAY' THEN StatusId END),
    @DELAY_FU_X = MAX(CASE WHEN StatusCode = 'DELAY_FU_X' THEN StatusId END),
    @DELAY_FU = MAX(CASE WHEN StatusCode = 'DELAY_FU' THEN StatusId END),
    @DELAY_FX = MAX(CASE WHEN StatusCode = 'DELAY_FX' THEN StatusId END)
FROM [LOG_FI_ALMT].administration.Status;
```

### 3. Build staging tables explicitly

```sql
CREATE TABLE #FS
(
    FeedSourceId int NOT NULL,
    SkSourceSystem int NOT NULL,
    PRIMARY KEY CLUSTERED (FeedSourceId, SkSourceSystem)
);

CREATE TABLE #Books
(
    SkNoBook int NOT NULL,
    SkMappingGroup int NOT NULL,
    PRIMARY KEY CLUSTERED (SkNoBook, SkMappingGroup)
);

CREATE TABLE #SignOffGroups
(
    SkMappingGroup int NOT NULL PRIMARY KEY CLUSTERED
);

CREATE TABLE #Scope
(
    PortfolioName nvarchar(256) NOT NULL,
    BusinessDataTypeId smallint NOT NULL,
    FeedSourceId int NOT NULL,
    FeedVersion int NULL,
    TypeOfCalculation char(1) NULL,
    PnlDate date NOT NULL,
    RejectedRowsNettoed bigint NULL,
    FeedRowCount bigint NULL,
    PortfolioLiquidPriceBalance decimal(38, 10) NULL,
    PostSyncStatus smallint NULL,
    IntegrateStatus smallint NULL,
    CurrentStep nvarchar(64) NULL,
    WorkflowPnLDailyOpen bit NULL,
    SynchroStatus smallint NULL,
    IsFailed bit NULL,
    EventTypeId int NULL,
    FlowIdDerivedFrom bigint NULL,
    FlowId bigint NOT NULL,
    INDEX IX_Scope_Main CLUSTERED (PortfolioName, BusinessDataTypeId, FeedSourceId, PnlDate, TypeOfCalculation)
);
```

### 4. Precompute date flags once

```sql
DECLARE @IsEndOfMonth int = [administration].[fn_DayIsEndOfMonth](@PnlDate);
DECLARE @IsWeek bit = administration.fn_IsWeek(0, @PnlDate);
DECLARE @LastEOM date = administration.fn_GetPreviousBusinessDate(NULL, DATEADD(day, 1, EOMONTH(DATEADD(month, -1, @PnlDate))));
```

Use those variables in the `CASE` status mapping instead of re-calling the functions per row.

### 5. Keep the final classification static

The final result query should remain a single static `SELECT` against staged tables. If there is still major parameter skew after moving filters into temp tables, add `OPTION (RECOMPILE)` at the final query level only.

## DBA Review Questions

1. Can the caller switch from CSV filters to TVPs?
2. Is `READ COMMITTED SNAPSHOT` already enabled on `LOG_FI_ALMT` and `STAGING_FI_ALMT`?
3. Does the current prod plan spend most of its time in the `Flows` load, the `#PortfolioSystemContribution` join, or the final classification?
4. Are there supporting indexes on `administration.Flows` for `(PnlDate, FeedSourceDerivedId, BusinessDataTypeId, FeedSourceId, PortfolioName)`?

## Expected Outcome If Approved

1. More stable execution plans.
2. Less risk of false monitoring statuses caused by dirty reads.
3. Lower tempdb pressure from repeated wide `DISTINCT` operations.
4. Better troubleshooting because the proc becomes stage-oriented instead of string-generated.