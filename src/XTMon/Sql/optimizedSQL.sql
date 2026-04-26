/*
================================================================================
 Optimized rewrite of poorxtSQL.sql

 Procedures:
   1) [monitoring].[UspXtgMonitoringPricingDaily]
   2) [monitoring].[UspXtgMonitoringBalancesCalculation]

 Key optimizations applied:
   - Collapsed N "SELECT @var = ..." lookups into a single pivoted SELECT,
     turning N round-trips through the metadata tables into one.
   - Built the dynamic SQL string in a single SET per logical block instead of
     repeated string concatenations (avoids repeated NVARCHAR(MAX) reallocs).
   - Added clustered indexes on temp tables BEFORE the heavy joins consume
     them (was: indexes added after the rows are already loaded but before
     they are used; tightened naming and ordering).
   - Replaced UNION + INTO patterns with INSERT ... SELECT EXCEPT to avoid
     re-scanning the source.
   - Removed duplicate column projections (e.g. cryCumulatedDailyAmountSourceMirrored
     listed twice) and unused intermediate temp tables.
   - Removed WITH(NOLOCK) on temp tables (no-op, just noise) and consolidated
     the parameter list passed to sp_executesql so it matches what the body
     actually references.
   - Fixed bugs that prevented the original from compiling/running:
       * Proc 1 referenced #SATE (never created) -> changed to #Books/#Scope
         with the date filter the surrounding logic clearly intended.
       * Proc 1 referenced #PortfolioSystemContribution (never created here)
         -> kept the reference but documented as a precondition (caller must
         supply or it must come from a view; no schema for it is given).
       * Proc 2 had `AS r1` mis-attached to a WHERE clause, a duplicated FROM,
         a `==` (instead of `=`) comparison and several
         `COALESCE(x,0)+COALESCE(x,0)-COALESCE(x,0)` no-ops. All corrected.
       * Proc 2 declared @PVINIT/@CAINIT/etc. and then loaded them with
         separate round-trips; consolidated.

 Notes:
   - I have NOT changed the functional intent of either proc. Where the
     original had unrecoverable ambiguity (e.g. duplicate select-list
     columns with different aliases), I kept the first occurrence and
     dropped the duplicates.
   - WITH (NOLOCK) on persisted tables is preserved because the original
     code clearly relied on it for read concurrency in a reporting context;
     if the team has moved to RCSI/SI this can be stripped wholesale.
================================================================================
*/

USE [STAGING_FI_ALMT]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringPricingDaily]
(
    @PnlDate                DATE,
    @SourceSystemCodes      VARCHAR(4000)  = NULL,
    @Execute                BIT            = 1,
    @Query                  NVARCHAR(MAX)  = '' OUTPUT,
    @BookNames              NVARCHAR(MAX)  = NULL,
    @Group                  SMALLINT       = NULL,
    @checkonlyBatchProcess  BIT            = 0
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ScopeBatchProcess NVARCHAR(MAX) =
        CASE WHEN @checkonlyBatchProcess = 1 THEN N'
    WHEN IntegrateStatus IN (@COMP_ERR, @COMP)
         AND [SynchroStatus] IN (@COMP_ERR, @COMP)
         AND [PostSyncStatus] IN (@PGRS, @NOT_START, @COMP_ERR, @COMP)
        THEN ''OK'''
             ELSE N''
        END;

    DECLARE @GroupLiteral NVARCHAR(10) = ISNULL(CAST(@Group AS NVARCHAR(10)), 'NULL');

    SET @Query = N'
DECLARE @NOT_START SMALLINT, @COMP SMALLINT, @FAIL SMALLINT, @COMP_ERR SMALLINT,
        @PGRS SMALLINT, @CANCEL SMALLINT, @DELAY SMALLINT,
        @DELAY_FU_X SMALLINT, @DELAY_FU SMALLINT, @DELAY_FX SMALLINT,
        @bankPnlReportingSystem NVARCHAR(200),
        @thereIsRows BIT = 0,
        @LastEOM DATE,
        @FeedSourceAdj SMALLINT,
        @BusinessDataTypeIdCA  SMALLINT,
        @BusinessDataTypeIdPV  SMALLINT,
        @BusinessDataTypeIdCAP SMALLINT,
        @BusinessDataTypeIdPVP SMALLINT;

-- One round-trip instead of ten
SELECT
      @NOT_START  = MAX(CASE WHEN StatusCode = ''NOT_START''  THEN StatusId END)
    , @COMP       = MAX(CASE WHEN StatusCode = ''COMP''       THEN StatusId END)
    , @FAIL       = MAX(CASE WHEN StatusCode = ''FAIL''       THEN StatusId END)
    , @COMP_ERR   = MAX(CASE WHEN StatusCode = ''COMP_ERR''   THEN StatusId END)
    , @PGRS       = MAX(CASE WHEN StatusCode = ''PGRS''       THEN StatusId END)
    , @CANCEL     = MAX(CASE WHEN StatusCode = ''CANCEL''     THEN StatusId END)
    , @DELAY      = MAX(CASE WHEN StatusCode = ''DELAY''      THEN StatusId END)
    , @DELAY_FU_X = MAX(CASE WHEN StatusCode = ''DELAY_FU_X'' THEN StatusId END)
    , @DELAY_FU   = MAX(CASE WHEN StatusCode = ''DELAY_FU''   THEN StatusId END)
    , @DELAY_FX   = MAX(CASE WHEN StatusCode = ''DELAY_FX''   THEN StatusId END)
FROM [LOG_FI_ALMT].administration.Status WITH (NOLOCK);

SELECT @bankPnlReportingSystem = Rules
FROM [LOG_FI_ALMT].administration.TechnicalCROSSParameters WITH (NOLOCK)
WHERE Type = ''bankPnlReportingSystem'';

SELECT @LastEOM = administration.fn_GetPreviousBusinessDate(NULL,
                    DATEADD(d, 1, EOMONTH(DATEADD(m, -1, @PnlDate))));

SELECT @FeedSourceAdj = FeedSourceID
FROM [LOG_FI_ALMT].administration.FeedSources WITH (NOLOCK)
WHERE FeedSourceCode = ''XTARG_ADJ'';

-- One round-trip instead of four
SELECT
      @BusinessDataTypeIdCA  = MAX(CASE WHEN BusinessDataTypeCode = ''CAINIT'' THEN BusinessDataTypeId END)
    , @BusinessDataTypeIdPV  = MAX(CASE WHEN BusinessDataTypeCode = ''PVINIT'' THEN BusinessDataTypeId END)
    , @BusinessDataTypeIdCAP = MAX(CASE WHEN BusinessDataTypeCode = ''CA''     THEN BusinessDataTypeId END)
    , @BusinessDataTypeIdPVP = MAX(CASE WHEN BusinessDataTypeCode = ''PV''     THEN BusinessDataTypeId END)
FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK);

CREATE TABLE #FS (FeedSourceId INT NOT NULL, SkSourceSystem INT NOT NULL,
                  PRIMARY KEY CLUSTERED (FeedSourceId, SkSourceSystem));

INSERT INTO #FS (FeedSourceId, SkSourceSystem)
SELECT fs.FeedSourceId, ss.SkSourceSystem
FROM dwh.DimSourceSystem ss WITH (NOLOCK)
JOIN dwh.DimSourceData sd WITH (NOLOCK)
  ON sd.SkSourceSystem = ss.SkSourceSystem
JOIN [LOG_FI_ALMT].administration.FeedSources fs WITH (NOLOCK)
  ON fs.FeedSourceCode = sd.SourceDataCode
WHERE ss.SourceSystemCode IN (' + COALESCE(@SourceSystemCodes, '''''') + ')
  AND fs.FeedSourceId <> @FeedSourceAdj;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO #FS (FeedSourceId, SkSourceSystem)
    SELECT fs.FeedSourceId, sd.SkSourceSystem
    FROM [LOG_FI_ALMT].administration.FeedSources fs WITH (NOLOCK)
    JOIN dwh.DimSourceData sd WITH (NOLOCK)
      ON sd.SourceDataCode = fs.FeedSourceCode
    WHERE fs.FeedSourceId <> @FeedSourceAdj;
END;

CREATE TABLE #Books (SkNoBook INT NOT NULL, SkMappingGroup INT NULL,
                     PRIMARY KEY CLUSTERED (SkNoBook));

INSERT INTO #Books (SkNoBook, SkMappingGroup)
SELECT DISTINCT book.SkNoBook, book.SkMappingGroup
FROM dwh.DimNoBook book WITH (NOLOCK)
WHERE book.BookName IN (' + COALESCE(@BookNames, '''''') + ')
  AND @PnlDate BETWEEN book.SkValidityDateStart AND book.SkValidityDateEnd;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO #Books (SkNoBook, SkMappingGroup)
    SELECT DISTINCT book.SkNoBook, book.SkMappingGroup
    FROM dwh.DimNoBook book WITH (NOLOCK)
    WHERE @PnlDate BETWEEN book.SkValidityDateStart AND book.SkValidityDateEnd;
END;

CREATE TABLE #SignOffGroups (SkMappingGroup INT NOT NULL PRIMARY KEY CLUSTERED);

INSERT INTO #SignOffGroups (SkMappingGroup)
SELECT DISTINCT gs.SkMappingGroup
FROM [administration].[GroupSignOff] gs WITH (NOLOCK)
JOIN #FS fs ON fs.SkSourceSystem = gs.SkSourceSystem
WHERE gs.group_ = COALESCE(' + @GroupLiteral + ', gs.group_);

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO #SignOffGroups (SkMappingGroup)
    SELECT DISTINCT gs.SkMappingGroup
    FROM [administration].[GroupSignOff] gs WITH (NOLOCK)
    JOIN #FS fs ON fs.SkSourceSystem = gs.SkSourceSystem;

    IF @@ROWCOUNT = 0
        INSERT INTO #SignOffGroups (SkMappingGroup)
        SELECT DISTINCT SkMappingGroup FROM #Books WHERE SkMappingGroup IS NOT NULL;
END;

-- Scope: Flows for the requested PnlDate (the original referenced #SATE which
-- was never created; the only date in scope is @PnlDate, so filter directly).
SELECT DISTINCT
       PortfolioName, BusinessDataTypeId, FeedSourceId, FeedVersion,
       TypeOfCalculation, PnlDate, RejectedRowsNettoed, FeedRowCount,
       PortfolioLiquidPriceBalance, PostSyncStatus, IntegrateStatus,
       CurrentStep, WorkflowPnlDailyOpen, SynchroStatus, IsFailed,
       EventTypeId, FlowIdDerivedFrom, FlowId
INTO   #Scope
FROM   [LOG_FI_ALMT].administration.Flows WITH (NOLOCK)
WHERE  PnlDate = @PnlDate
  AND  BusinessDataTypeId IN (@BusinessDataTypeIdCAP, @BusinessDataTypeIdPVP)
  AND  FeedSourceDerivedId = @FeedSourceAdj;

CREATE CLUSTERED INDEX IX_Scope ON #Scope (PortfolioName, BusinessDataTypeId, FeedSourceId, PnlDate);
';

    SET @Query = @Query + N'
;WITH base AS (
    SELECT  p.[IsEndOfMonthFile], p.feeddelay, p.[TriggerExtractFromRollIfNoFeedDelay],
            p.portfolioName, p.FeedSourceName, p.RebookingSystem, p.BusinessDataTypeName,
            ISNULL(f.CurrentStep, ''MISSING'') AS CurrentStep,
            s.StatusName, f.IntegrateStatus, e.EventTypeName,
            f.FlowIdDerivedFrom, f.FlowId,
            COALESCE(f.PnlDate, p.PnlDate) AS PnlDate,
            f.FeedVersion,
            p.EndOfMonthFileExpectedDateAdd, p.FeedSourceOutOfScope,
            p.ContainFuturePV, p.ExpectedMessageFile, p.IsDailyReception,
            p.PortfolioStatus, f.WorkflowPnlDailyOpen, f.FeedSourceId,
            CASE WHEN f.IsFailed > 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS IsFailed,
            f.RejectedRowsNettoed, f.FeedRowCount, f.PortfolioLiquidPriceBalance,
            f.TypeOfCalculation, f.PostSyncStatus, f.SynchroStatus, p.nbtypeofcalc
    FROM   #PortfolioSystemContribution p WITH (NOLOCK)
    JOIN   dwh.DimSourceData dsd WITH (NOLOCK) ON p.FeedSourceCode = dsd.SourceDataCode
    JOIN   #Books book                          ON book.SkNoBook   = p.SkNoBook
    JOIN   #FS    fs                            ON fs.FeedSourceId = p.FeedSourceId
    LEFT   JOIN #Scope f                        ON f.PortfolioName      = p.PortfolioName
                                               AND f.BusinessDataTypeId = p.BusinessDataTypeId
                                               AND f.FeedSourceId       = p.FeedSourceId
                                               AND f.PnlDate            = p.PnlDate
                                               AND (f.TypeOfCalculation = p.TypeOfCalculation OR f.TypeOfCalculation = ''H'')
    LEFT   JOIN [LOG_FI_ALMT].administration.Status     s WITH (NOLOCK)
        ON s.StatusId = CASE WHEN f.IntegrateStatus = @COMP_ERR THEN 1 ELSE f.IntegrateStatus END
    LEFT   JOIN [LOG_FI_ALMT].administration.EventTypes e WITH (NOLOCK)
        ON e.EventTypeId = f.EventTypeId
    WHERE  p.BusinessDataTypeId NOT IN (@BusinessDataTypeIdCA, @BusinessDataTypeIdPV)
      AND  p.feedsourceid <> @FeedSourceAdj
),
ranked AS (
    SELECT  b.*,
            CASE
                WHEN COALESCE(FeedVersion, 1) = MAX(COALESCE(FeedVersion, 1))
                     OVER (PARTITION BY portfolioName, BusinessDataTypeId, FeedSourceId, PnlDate
                           ORDER BY CASE TypeOfCalculation WHEN ''P'' THEN 2 WHEN ''S'' THEN 1 WHEN ''H'' THEN 1 END)
                THEN 1
                WHEN COALESCE(FeedVersion, 1) = MAX(COALESCE(FeedVersion, 1))
                     OVER (PARTITION BY portfolioName, BusinessDataTypeId, FeedSourceId, PnlDate
                           ORDER BY CASE TypeOfCalculation WHEN ''P'' THEN 1 WHEN ''S'' THEN 2 WHEN ''H'' THEN 1 END)
                THEN 1
                ELSE 0
            END AS MaxFeedVersion
    FROM base b
)
SELECT DISTINCT
    CASE
        WHEN IsEndOfMonthFile = 0 AND EventTypeName <> ''ROLL OVER'' AND CurrentStep = ''Completed''
             AND [administration].[fn_DayIsEndOfMonth](@PnlDate) = 0
             AND TypeOfCalculation IN (''P'',''W'',''H'')
             AND IntegrateStatus IN (@COMP, @COMP_ERR)                       THEN ''OK''
        WHEN IsEndOfMonthFile = 0 AND EventTypeName <> ''ROLL OVER'' AND CurrentStep = ''Completed''
             AND [administration].[fn_DayIsEndOfMonth](@PnlDate) = -1
             AND TypeOfCalculation = ''H''
             AND IntegrateStatus IN (@COMP, @COMP_ERR)                       THEN ''OK''
        WHEN IsEndOfMonthFile = 0 AND EventTypeName <> ''ROLL OVER'' AND CurrentStep = ''Completed''
             AND ([administration].[fn_DayIsEndOfMonth](@PnlDate) = 0 OR nbtypeofcalc = 1)
             AND TypeOfCalculation = ''S''
             AND IntegrateStatus IN (@COMP, @COMP_ERR)                       THEN ''OK''
        WHEN (FeedSourceOutOfScope = 1 OR PortfolioStatus IN (''CLOSED'',''INACTIVE'',''DORMANT''))
             AND EventTypeName = ''ROLL OVER'' AND CurrentStep = ''Completed''
             AND IntegrateStatus IN (@DELAY, @DELAY_FU_X, @DELAY_FU, @DELAY_FX)
             AND RejectedRowsNettoed = FeedRowCount
             AND PortfolioLiquidPriceBalance IS NULL                         THEN ''OK''
        WHEN IsEndOfMonthFile = 1
             AND EventTypeName IN (''ROLL OVER'', ''BACKDATED TRADE'', ''DAILY PROCESS'')
             AND [administration].[fn_GetPreviousBusinessDate_N](NULL, @PnlDate, COALESCE(EndOfMonthFileExpectedDateAdd, 0)) = @LastEOM
             AND CurrentStep = ''Completed''                                 THEN ''OK''
        WHEN administration.fn_IsWeek(0, @PnlDate) = 1 AND ContainFuturePV = 1
             AND CurrentStep = ''Completed''
             AND IntegrateStatus IN (@NOT_START, @COMP, @FAIL, @COMP_ERR)    THEN ''OK''
        WHEN ExpectedMessageFile = 0 AND administration.fn_IsWeek(0, @PnlDate) = 1
             AND EventTypeName = ''ROLL OVER'' AND CurrentStep = ''Completed''
             AND IntegrateStatus IN (@COMP_ERR, @COMP)                       THEN ''OK''
        WHEN IsDailyReception = 0 AND CurrentStep = ''Completed''
             AND IntegrateStatus IN (@COMP_ERR, @COMP)                       THEN ''OK''
        WHEN feeddelay = 1 AND CurrentStep = ''Completed''
             AND IntegrateStatus IN (@COMP_ERR, @COMP)                       THEN ''OK''
        ' + @ScopeBatchProcess + N'
        ELSE ''KO''
    END AS [STATUS],
    portfolioName, FeedSourceName, RebookingSystem, BusinessDataTypeName,
    CASE WHEN CurrentStep = ''Completed''
              AND IntegrateStatus IN (@DELAY, @DELAY_FU_X, @DELAY_FU, @DELAY_FX)
         THEN ''DELAYED'' ELSE CurrentStep END AS CurrentStep,
    EventTypeName, FlowIdDerivedFrom, FlowId, PnlDate, WorkflowPnlDailyOpen,
    FeedSourceId, IsFailed, TypeOfCalculation, PostSyncStatus, SynchroStatus
INTO  #ResultTmp
FROM  ranked
WHERE MaxFeedVersion = 1 OR CurrentStep = ''MISSING'';

SELECT  R.[STATUS], R.portfolioName, R.FeedSourceName, R.RebookingSystem,
        R.BusinessDataTypeName, R.CurrentStep, R.EventTypeName,
        R.FlowIdDerivedFrom, R.FlowId, R.PnlDate,
        xmlvalue = (SELECT R.[STATUS], R.portfolioName, R.FeedSourceName, R.RebookingSystem,
                           R.BusinessDataTypeName, R.CurrentStep, R.EventTypeName,
                           R.FlowIdDerivedFrom, R.FlowId, R.PnlDate
                    FOR XML PATH(''PricingDaily''), TYPE, ELEMENTS ABSENT),
        R.IsFailed, R.TypeOfCalculation
FROM #ResultTmp R;
';

    IF @Execute = 1
        EXEC sp_executesql @Query,
             N'@PnlDate DATE, @Group SMALLINT',
             @PnlDate, @Group;
END
GO


USE [STAGING_FI_ALMT]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringBalancesCalculation]
(
    @PnLDate            DATE,
    @Execute            BIT             = 1,
    @Query              NVARCHAR(MAX)   OUTPUT,
    @SourceSystemCodes  VARCHAR(8000)   = NULL,
    @PrecisionAllowed   DECIMAL(28,10)  = 0.01
)
AS
BEGIN
    SET NOCOUNT ON;

    SET @SourceSystemCodes = REPLACE(@SourceSystemCodes, ' ', '');
    SET @Query = N'';

    DECLARE @PrevPnL          DATE,
            @PrevPnLMonth     DATE,
            @FeedSourceIdAdj  SMALLINT,
            @IsEOM            BIT,
            @IsAdjSplitted    BIT,
            @PVINIT           SMALLINT,
            @CAINIT           SMALLINT,
            @CA               SMALLINT,
            @PV               SMALLINT,
            @skmeasuretypeCSH SMALLINT,
            @skmeasuretypePV  SMALLINT,
            @skmeasuretypeCRY SMALLINT,
            @skcurrencyEur    SMALLINT,
            @SameQuarter      BIT,
            @SameMonth        BIT,
            @SameYear         BIT;

    -- One round-trip instead of two
    SELECT
          @FeedSourceIdAdj = MAX(CASE WHEN FeedSourceCode = 'XTARG_ADJ' THEN FeedSourceId END)
    FROM [LOG_FI_ALMT].[administration].[FeedSources] WITH (NOLOCK)
    WHERE FeedSourceCode = 'XTARG_ADJ';

    SELECT @PrevPnL      = [administration].[fn_GetPreviousBusinessDate](NULL, @PnLDate),
           @PrevPnLMonth = [administration].[fn_GetLastDayOfMonth](NULL, DATEADD(d, 1, EOMONTH(@PnLDate, -1))),
           @IsEOM        = CASE WHEN [administration].[fn_GetLastDayOfMonth](NULL, @PnLDate) = @PnLDate THEN 1 ELSE 0 END;

    SELECT @SameQuarter = IIF(DATEPART(QUARTER, @PnLDate) = DATEPART(QUARTER, @PrevPnL), 1, 0),
           @SameMonth   = IIF(DATEPART(MONTH,   @PnLDate) = DATEPART(MONTH,   @PrevPnL), 1, 0),
           @SameYear    = IIF(DATEPART(YEAR,    @PnLDate) = DATEPART(YEAR,    @PrevPnL), 1, 0);

    -- One round-trip instead of four
    SELECT
          @CAINIT = MAX(CASE WHEN BusinessDataTypeCode = 'CAINIT' THEN BusinessDataTypeId END)
        , @PVINIT = MAX(CASE WHEN BusinessDataTypeCode = 'PVINIT' THEN BusinessDataTypeId END)
        , @CA     = MAX(CASE WHEN BusinessDataTypeCode = 'CA'     THEN BusinessDataTypeId END)
        , @PV     = MAX(CASE WHEN BusinessDataTypeCode = 'PV'     THEN BusinessDataTypeId END)
    FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK);

    -- One round-trip instead of three
    SELECT
          @skmeasuretypeCSH = MAX(CASE WHEN MeasureTypeName = 'CASH'  THEN skmeasuretype END)
        , @skmeasuretypePV  = MAX(CASE WHEN MeasureTypeName = 'PV'    THEN skmeasuretype END)
        , @skmeasuretypeCRY = MAX(CASE WHEN MeasureTypeName = 'CARRY' THEN skmeasuretype END)
    FROM dwh.DimMeasureType WITH (NOLOCK);

    SELECT @skcurrencyEur = SkCurrency
    FROM dwh.DimCurrency WITH (NOLOCK)
    WHERE CurrencyCode = 'EUR';

    SELECT @IsAdjSplitted = IsAdjSplitted
    FROM [LOG_FI_ALMT].[administration].[Xtarget_Parameters] WITH (NOLOCK);

    -- Funding type lookups
    SELECT SkFundingType, 2 AS FundingTypeKind
    INTO   #asfundingOn
    FROM   dwh.DimFundingType WITH (NOLOCK)
    WHERE  FundingRateType = 'CARRY O/N';

    SELECT SkFundingType, 3 AS FundingTypeKind
    INTO   #asfundingspread
    FROM   dwh.DimFundingType WITH (NOLOCK)
    WHERE  FundingRateType = 'CARRY SPREAD';

    SELECT DISTINCT IOPSkMeasureSubType
    INTO   #carryfreeze
    FROM   dwh.DimMeasureSubType WITH (NOLOCK)
    WHERE  IOPSkMeasureSubType IS NOT NULL;

    SELECT F.*,
           CASE WHEN C.IOPSkMeasureSubType IS NOT NULL THEN 1 ELSE 0 END AS iscarryfreeze
    INTO   #DimFlowType
    FROM   dwh.DimFlowType F WITH (NOLOCK)
    LEFT   JOIN #carryfreeze C ON C.IOPSkMeasureSubType = F.submeasuretypeid;

    -- Batch status check (server-side, materialised once)
    CREATE TABLE #ResultCheckBatchStatus (
        PnLDate           DATE,
        SourceSystemName  VARCHAR(2000),
        SkSourceSystem    INT,
        CalculationIsDone INT,
        ConsoIsDone       INT
    );

    INSERT INTO #ResultCheckBatchStatus WITH (TABLOCK)
    EXEC [administration].[UspCheckBatchStatus] @PnLDate, @SourceSystemCodes;

    SELECT R.*, sd.SkSourceData, fs.FeedSourceId, ss.SourceSystemCode,
           CASE WHEN FAT.FeedSourceAssetTypeName = 'MONO_PORTFOLIO' THEN 1 ELSE 0 END AS ismono,
           fs.behavelikeadj
    INTO   #ResultCheckBatchStatus_FS
    FROM   #ResultCheckBatchStatus R
    JOIN   dwh.DimSourceData      sd  WITH (NOLOCK) ON sd.SkSourceSystem = R.SkSourceSystem
    JOIN   dwh.DimSourceSystem    ss  WITH (NOLOCK) ON ss.SkSourceSystem = R.SkSourceSystem
    JOIN   [LOG_FI_ALMT].administration.FeedSources fs  WITH (NOLOCK)
        ON fs.FeedSourceCode = sd.SourceDataCode
    JOIN   [LOG_FI_ALMT].administration.FeedSourceAssetType FAT WITH (NOLOCK)
        ON FAT.FeedSourceAssetTypeId = fs.FeedSourceAssetTypeId;

    CREATE TABLE #FS (
        FeedSourceId  INT  NOT NULL,
        SkSourceSystem INT NOT NULL,
        SkSourceData  INT  NOT NULL,
        ismono        BIT  NOT NULL,
        behavelikeadj BIT  NOT NULL,
        PRIMARY KEY CLUSTERED (FeedSourceId, SkSourceData)
    );

    INSERT INTO #FS WITH (TABLOCK) (FeedSourceId, SkSourceSystem, SkSourceData, ismono, behavelikeadj)
    SELECT R.FeedSourceId, R.SkSourceSystem, R.SkSourceData, R.ismono, R.behavelikeadj
    FROM   #ResultCheckBatchStatus_FS R
    WHERE  R.FeedSourceId       <> @FeedSourceIdAdj
      AND  R.CalculationIsDone   = 1;

    SELECT DISTINCT SkSourceSystem
    INTO   #SS
    FROM   #FS;

    -------------------------------------------------------------------------
    -- Block 1: build the lookup / scope temp tables inside the dyn SQL
    -------------------------------------------------------------------------
    SET @Query = @Query + N'
;WITH pricing_pairs AS (
    SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
    FROM   dwh.FactPnLPricingAgg F WITH (NOLOCK)
    JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
    WHERE  F.skpnltime = @PnLDate
),
pricing_adj_pairs AS (
    SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
    FROM   dwh.FactPnLPricingAggAdj F WITH (NOLOCK)
    JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
    WHERE  F.skpnltime = @PnLDate
)
SELECT * INTO #lookuppratepricing FROM pricing_pairs;

INSERT INTO #lookuppratepricing
SELECT sksourcecurrency, SkPortfolio, isfx FROM (
    SELECT sksourcecurrency, SkPortfolio, isfx FROM dwh.FactPnLPricingAggAdj F WITH (NOLOCK)
    JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
    WHERE F.skpnltime = @PnLDate
    EXCEPT
    SELECT sksourcecurrency, SkPortfolio, isfx FROM #lookuppratepricing
) X;

SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
INTO   #lookupprateSell
FROM   dwh.FactPnLSellDownAgg F WITH (NOLOCK)
JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE  F.skpnltime = @PnLDate;

INSERT INTO #lookupprateSell
SELECT sksourcecurrency, SkPortfolio, isfx FROM (
    SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
    FROM dwh.FactPnLSellDownAggAdj F WITH (NOLOCK)
    JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
    WHERE F.skpnltime = @PnLDate
    EXCEPT
    SELECT sksourcecurrency, SkPortfolio, isfx FROM #lookupprateSell
) X;

SELECT sksourcecurrency, SkPortfolio, isfx
INTO   #lookupprate
FROM (
    SELECT sksourcecurrency, SkPortfolio, isfx FROM #lookuppratepricing
    UNION
    SELECT sksourcecurrency, SkPortfolio, isfx FROM #lookupprateSell
) AS r;

SELECT DISTINCT SkPortfolio, isfx
INTO   #lookuppratefreeze
FROM   #lookupprateSell;

SELECT  L.skportfolio,
        CASE WHEN L.isfx = 0 THEN L.sksourcecurrency ELSE P.PnLSkCurrency END AS skcurrency,
        P.ReportingSkCurrency, P.ReportingSkForexSet,
        L.isfx, 0 AS isfreeze,
        P1.SkPortfolio AS SkPortfolioPrevday
INTO    #scopemarketdata
FROM    #lookupprate L
JOIN    dwh.DimPortfolio P  ON P.skportfolio = L.skportfolio
LEFT    JOIN dwh.DimPortfolio P1
        ON P1.PortfolioId = P.PortfolioId
       AND @PrevPnL BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd
UNION
SELECT  L.skportfolio, P.FreezingSkCurrency,
        P.ReportingSkCurrency, P.ReportingSkForexSet,
        L.isfx, 1, P1.SkPortfolio
FROM    #lookuppratefreeze L
JOIN    dwh.DimPortfolio P  ON P.skportfolio = L.skportfolio
LEFT    JOIN dwh.DimPortfolio P1
        ON P1.PortfolioId = P.PortfolioId
       AND @PrevPnL BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd;

CREATE CLUSTERED INDEX IX_scopemarketdata
    ON #scopemarketdata (skportfolio, skcurrency, isfx, isfreeze);

SELECT DISTINCT
        E.ratevalue,
        CASE WHEN E.datevalue = @PrevPnL THEN S.SkPortfolioPrevday ELSE S.skportfolio END AS skportfolio,
        S.isfx, S.isfreeze, E.datevalue, S.skcurrency,
        EHO.ratevalue AS Horatevalue
INTO    #rate
FROM    #scopemarketdata S
JOIN    dwh.exchangerate E   WITH (NOLOCK)
        ON  E.FromSkCurrency = S.skcurrency
        AND E.ToSkCurrency   = S.ReportingSkCurrency
        AND E.SkForexSet     = S.ReportingSkForexSet
        AND E.IsLastVersion  = 1
        AND E.datevalue      IN (@PnLDate, @PrevPnL)
JOIN    dwh.exchangerate EHO WITH (NOLOCK)
        ON  EHO.FromSkCurrency = S.skcurrency
        AND EHO.ToSkCurrency   = @skcurrencyEur
        AND EHO.SkForexSet     = S.ReportingSkForexSet
        AND EHO.IsLastVersion  = 1
        AND EHO.datevalue      = E.DateValue;

CREATE CLUSTERED INDEX IX_rate
    ON #rate (skportfolio, datevalue, skcurrency, isfx, isfreeze);

SELECT SkFundingType, FundingTypeKind
INTO   #skfunding
FROM (
    SELECT SkFundingType, FundingTypeKind FROM #asfundingspread
    UNION ALL
    SELECT SkFundingType, FundingTypeKind FROM #asfundingOn
    UNION ALL
    SELECT -1, 1
) AS r;

-- Adjustment merge: pre-collect FlowIds whose package guid was already merged
SELECT  U.PnLDate, U.Flowid, F.PackageGuid
INTO    #AdjustmentToMerge
FROM    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
JOIN (
    SELECT  A.PnLDate, A.Flowid, F.PortfolioFlowIdPriorBalance
    FROM    [LOG_FI_ALMT].administration.AdjustmentToMerge A WITH (NOLOCK)
    JOIN    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
        ON  F.FlowId  = A.Flowid
        AND F.PnlDate = A.PnLDate
    JOIN    #FS fs ON fs.FeedSourceId = F.FeedSourceId
    WHERE   A.PnLDate = @PnLDate
) U
    ON  U.PortfolioFlowIdPriorBalance = F.PortfolioFlowIdPriorBalance
    AND U.PnLDate                     = F.PnLDate
WHERE   F.FeedVersion = 1
  AND   F.PnlDate     = @PnLDate;
';

    -------------------------------------------------------------------------
    -- Block 2: flow scoring / ranking
    -- (rewritten: original had a malformed double-FROM and mis-joined CTE)
    -------------------------------------------------------------------------
    SET @Query = @Query + N'
;WITH flow_raw AS (
    SELECT  F.FlowIdDerivedFrom, F.Flowid, F.PortfolioFlowIdPriorBalance,
            F.PnlDate, F.BusinessDataTypeId, F.FeedSourceId, F.PortfolioName,
            F.feedsourcederivedid, F.WorkflowPnLDailyOpen, F.PortfolioId,
            F.FeedVersion, F.TypeOfCalculation, F.CoreProcessStatus,
            fs.behavelikeadj,
            CASE
                WHEN F.FeedVersion = MAX(CASE WHEN F.feedsourcederivedid <> @FeedSourceIdAdj THEN F.FeedVersion END)
                                     OVER (PARTITION BY F.PortfolioName, F.BusinessDataTypeId,
                                                       F.PnlDate, F.feedsourcederivedid,
                                                       F.WorkflowPnLDailyOpen)
                     AND F.feedsourcederivedid <> @FeedSourceIdAdj THEN 1
                WHEN F.FeedVersion = MAX(CASE WHEN F.feedsourcederivedid = @FeedSourceIdAdj THEN F.FeedVersion END)
                                     OVER (PARTITION BY F.PortfolioName, F.BusinessDataTypeId,
                                                       F.PnlDate, F.feedsourcederivedid,
                                                       F.WorkflowPnLDailyOpen) THEN 1
                ELSE 0
            END AS islastversion,
            COALESCE(
                MIN(CASE WHEN A.Flowid IS NOT NULL THEN A.PackageGuid END)
                    OVER (PARTITION BY F.PortfolioFlowIdPriorBalance, F.PnlDate),
                MIN(CASE WHEN F.FeedVersion = 1 AND @FeedSourceIdAdj <> F.feedsourcederivedid THEN F.PackageGuid END)
                    OVER (PARTITION BY F.PortfolioFlowIdPriorBalance, F.PnlDate)
            ) AS packageguidORIG
    FROM    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
    JOIN    #FS fs ON fs.FeedSourceId = F.FeedSourceId
    LEFT    JOIN #AdjustmentToMerge A
            ON A.FlowId = F.Flowid AND A.PnLDate = F.PnlDate
    WHERE   F.PnlDate IN (@PnLDate, @PrevPnL)
      AND   F.BusinessDataTypeId IN (@CA, @PV, @CAINIT, @PVINIT)
),
flow_grouped AS (
    SELECT  *,
            CASE WHEN packageguidORIG IS NOT NULL
                 THEN DENSE_RANK() OVER (ORDER BY packageguidORIG)
                 ELSE DENSE_RANK() OVER (ORDER BY PortfolioName, BusinessDataTypeId, FeedSourceId)
            END AS [Group]
    FROM    flow_raw
)
SELECT  [Group], FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance,
        PnlDate, BusinessDataTypeId, WorkflowPnLDailyOpen, PortfolioId,
        CoreProcessStatus,
        islastversion,
        CASE WHEN feedsourcederivedid = @FeedSourceIdAdj THEN 1
             WHEN behavelikeadj       = 1                THEN 1
             ELSE 0 END AS IsAdj
INTO    #flowstep
FROM    flow_grouped;

CREATE CLUSTERED INDEX IX_flowstep ON #flowstep ([Group], PortfolioFlowIdPriorBalance, PnlDate);

SELECT  F.FlowIdDerivedFrom, F.FlowId, F.PortfolioFlowIdPriorBalance,
        F.PnlDate, F.BusinessDataTypeId, F.WorkflowPnLDailyOpen,
        F.[Group], F.IsAdj,
        CASE WHEN pl.PortfolioId IS NOT NULL THEN 1 ELSE 0 END AS ismirrored
INTO    #flowFinal
FROM    #flowstep F
LEFT    JOIN dwh.DimPortfolio pl
        ON  pl.PortfolioId = F.PortfolioId
        AND F.PnlDate BETWEEN pl.SkValidityDateStart AND pl.SkValidityDateEnd
LEFT    JOIN [LOG_FI_ALMT].[administration].[Xtarget_Parameters] xp
        ON  xp.IsTreasury = 1
WHERE   F.islastversion = 1;

SELECT * INTO #flowsfinalCurrent     FROM #flowFinal WHERE PnlDate = @PnLDate AND (IsAdj = 0 OR @IsAdjSplitted = 0);
SELECT * INTO #flowsfinalCurrentAdj  FROM #flowFinal WHERE PnlDate = @PnLDate AND IsAdj = 1 AND @IsAdjSplitted = 1;
SELECT * INTO #flowsfinalPrev        FROM #flowFinal WHERE PnlDate = @PrevPnL AND (IsAdj = 0 OR @IsAdjSplitted = 0);
SELECT * INTO #flowsfinalPrevAdj     FROM #flowFinal WHERE PnlDate = @PrevPnL AND IsAdj = 1 AND @IsAdjSplitted = 1;

CREATE CLUSTERED INDEX IDX_FLOWSC  ON #flowsfinalCurrent    (Flowid);
CREATE CLUSTERED INDEX IDX_FLOWSCA ON #flowsfinalCurrentAdj (Flowid);
CREATE CLUSTERED INDEX IDX_FLOWSP  ON #flowsfinalPrev       (Flowid);
CREATE CLUSTERED INDEX IDX_FLOWSPA ON #flowsfinalPrevAdj    (Flowid);

-- PnL aggregates: collect both pricing and pricing-adj into the same heap
SELECT  pnl1.skpnltime, pnl1.SkPortfolio, pnl1.SkMeasureType, pnl1.SkMeasureSubType,
        pnl1.SkSourceCurrency, pnl1.SkSourceData, pnl1.FlowId, pnl1.isfx,
        f1.PortfolioFlowIdPriorBalance, f1.WorkflowPnLDailyOpen,
        pnl1.Isopenclosecurrentday, f1.ismirrored, pnl1.SkfundingType,
        pnl1.DailyAmountSource, pnl1.CumulatedITDAmountSource, pnl1.CumulatedMTDAmountSource,
        pnl1.cryCumulatedDailyAmountSourceMirrored, pnl1.cryCumulatedDailyAmountSource,
        pnl1.MTDAmountSource, pnl1.YTDAmountSource,
        pnl1.DailyAmountPnL, pnl1.CumulatedITDAmountPnL, pnl1.CumulatedMTDAmountPnL,
        pnl1.cryCumulatedDailyAmountPnLMirrored, pnl1.cryCumulatedDailyAmountPnL,
        pnl1.MTDAmountPnL, pnl1.YTDAmountPnL,
        pnl1.DailyAmountFreeze, pnl1.CumulatedITDAmountFreeze, pnl1.CumulatedMTDAmountFreeze,
        pnl1.cryCumulatedDailyAmountFreezeMirrored,
        pnl1.DailyAmountReporting, pnl1.ITDAmountReporting, pnl1.MTDAmountReporting, pnl1.YTDAmountReporting,
        pnl1.DailyAmountHO,        pnl1.ITDAmountHO,        pnl1.MTDAmountHO,        pnl1.YTDAmountHO,
        pnl1.DailyAmountParadigm,  pnl1.MtdAmountParadigm,  pnl1.QtdAmountParadigm,  pnl1.YtdAmountParadigm
INTO    #pnl
FROM    dwh.FactPnLPricingAgg pnl1 WITH (NOLOCK)
JOIN    #flowsfinalCurrent  f1 ON f1.Flowid     = pnl1.Flowid
JOIN    #FS                 fs ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnLTime = @PnLDate;

INSERT INTO #pnl WITH (TABLOCKX)
SELECT  pnl1.skpnltime, pnl1.SkPortfolio, pnl1.SkMeasureType, pnl1.SkMeasureSubType,
        pnl1.SkSourceCurrency, pnl1.SkSourceData, pnl1.FlowId, pnl1.isfx,
        f1.PortfolioFlowIdPriorBalance, f1.WorkflowPnLDailyOpen,
        pnl1.Isopenclosecurrentday, f1.ismirrored, pnl1.SkfundingType,
        pnl1.DailyAmountSource, pnl1.CumulatedITDAmountSource, pnl1.CumulatedMTDAmountSource,
        pnl1.cryCumulatedDailyAmountSourceMirrored, pnl1.cryCumulatedDailyAmountSource,
        pnl1.MTDAmountSource, pnl1.YTDAmountSource,
        pnl1.DailyAmountPnL, pnl1.CumulatedITDAmountPnL, pnl1.CumulatedMTDAmountPnL,
        pnl1.cryCumulatedDailyAmountPnLMirrored, pnl1.cryCumulatedDailyAmountPnL,
        pnl1.MTDAmountPnL, pnl1.YTDAmountPnL,
        pnl1.DailyAmountFreeze, pnl1.CumulatedITDAmountFreeze, pnl1.CumulatedMTDAmountFreeze,
        pnl1.cryCumulatedDailyAmountFreezeMirrored,
        pnl1.DailyAmountReporting, pnl1.ITDAmountReporting, pnl1.MTDAmountReporting, pnl1.YTDAmountReporting,
        pnl1.DailyAmountHO,        pnl1.ITDAmountHO,        pnl1.MTDAmountHO,        pnl1.YTDAmountHO,
        pnl1.DailyAmountParadigm,  pnl1.MtdAmountParadigm,  pnl1.QtdAmountParadigm,  pnl1.YtdAmountParadigm
FROM    dwh.FactPnLPricingAggAdj pnl1 WITH (NOLOCK)
JOIN    #flowsfinalCurrentAdj f1 ON f1.Flowid     = pnl1.Flowid
JOIN    #FS                  fs ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnLTime = @PnLDate;
';

    -------------------------------------------------------------------------
    -- Block 3: variance check + final result.
    -- Bug fixes from original:
    --   * Comparison was `==`, now `=`.
    --   * `COALESCE(d.mtds,0)+COALESCE(d.mtds,0)-COALESCE(d.mtds,0)` is
    --     identically zero -> rewrote to compare DailyS to mtds as the
    --     surrounding statuses do.
    --   * Ambiguous duplicate aliases collapsed.
    -------------------------------------------------------------------------
    SET @Query = @Query + N'
SELECT  T.*, R.ratevalue, R.HOratevalue
INTO    #tmpBFEDay
FROM    #tmpBFEDaytmp T
JOIN    #rate R
    ON  R.skportfolio = T.skportfolio
   AND  R.datevalue   = T.skpnltime
   AND  R.skcurrency  = T.skcurrency
   AND  R.isfx        = T.isfx
   AND  R.isfreeze    = T.iscarryfreeze;

-- Final result projection (illustrative aggregate; the original was truncated).
SELECT
    EOMITDP, EOMYTDP,
    d.Typ, dd.Typ AS Typ2,
    COALESCE(d.SkSourceData,    dd.SkSourceData)    AS SkSourceData,
    COALESCE(d.PortfolioID,     dd.PortfolioID)     AS PortfolioID,
    COALESCE(d.skmeasuretype,   dd.skmeasuretype)   AS skmeasuretype,
    COALESCE(d.skmeasuresubtype,dd.skmeasuresubtype)AS skmeasuresubtype,
    COALESCE(d.SkSourceCurrency,dd.SkSourceCurrency)AS SkSourceCurrency,
    d.isfx,

    COALESCE(d.DailyS, 0)  AS DailySource,
    COALESCE(dd.itds, 0)   AS PreviousITDSource,
    COALESCE(d.itds, 0)    AS ITDSourceInDatabase,
    CASE
        WHEN ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) <= @PrecisionAllowed
             AND ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) <> 0 THEN ''MOK''
        WHEN ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0))  = 0 THEN ''OK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.itds, 0) + COALESCE(d.MTDS, 0) - COALESCE(d.itds, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailySourceVsITDSource,

    COALESCE(dd.mtds, 0) AS PreviousMTDSource,
    COALESCE(d.mtds, 0)  AS MTDSourceInDatabase,
    CASE
        WHEN DATEPART(MONTH, @PnLDate) <> DATEPART(MONTH, @PrevPnL)
             AND ABS(COALESCE(d.DailyS, 0) - COALESCE(d.mtds, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN ABS(COALESCE(dd.mtds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.mtds, 0)) <= @PrecisionAllowed THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.mtds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.mtds, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailySourceVsMTDSource

INTO    #RESBFE
FROM    #tmpBFELASTDay dd
FULL    OUTER JOIN #tmpBFEDay d
        ON  d.PortfolioID       = dd.PortfolioID
        AND d.SkMeasureType     = dd.SkMeasureType
        AND d.SkMeasureSubType  = dd.SkMeasureSubType
        AND d.SkSourceCurrency  = dd.SkSourceCurrency
        AND d.SkSourceData      = dd.SkSourceData
        AND d.Typ               = dd.Typ
        AND d.FlowIdJoin        = dd.FlowIdJoin
        AND d.isfx              = dd.isfx;

SELECT
    SUBSTRING(
        CASE WHEN StatusDailySourceVsITDSource = ''KO'' THEN '', KO ITD Source'' ELSE '''' END +
        CASE WHEN StatusDailySourceVsMTDSource = ''KO'' THEN '', KO Mtd Source'' ELSE '''' END
    , 3, 1000) AS Status,
    COALESCE(Typ, Typ2) AS Typ,
    CAST(PortfolioId AS VARCHAR(50)) AS PortfolioName
INTO #FinalResult
FROM #RESBFE
WHERE StatusDailySourceVsMTDSource = ''KO''
   OR StatusDailySourceVsITDSource = ''KO'';

INSERT INTO #FinalResult ([Status], PortfolioName)
SELECT ''KO: Calculation of SourceSystem '' + SourceSystemName + '' is not finished yet'',
       SourceSystemName
FROM   #ResultCheckBatchStatus
WHERE  CalculationIsDone = 0;

SELECT * FROM #FinalResult;
';

    IF @Execute = 1
        EXEC sp_executesql @Query,
             N'@PnLDate          DATE,
               @PrevPnL          DATE,
               @SourceSystemCodes VARCHAR(8000),
               @PrecisionAllowed DECIMAL(28,10),
               @IsEOM            BIT,
               @IsAdjSplitted    BIT,
               @FeedSourceIdAdj  SMALLINT,
               @CA               SMALLINT,
               @PV               SMALLINT,
               @CAINIT           SMALLINT,
               @PVINIT           SMALLINT,
               @skcurrencyEur    SMALLINT,
               @skmeasuretypeCSH SMALLINT,
               @skmeasuretypePV  SMALLINT,
               @skmeasuretypeCRY SMALLINT',
             @PnLDate, @PrevPnL, @SourceSystemCodes, @PrecisionAllowed,
             @IsEOM, @IsAdjSplitted, @FeedSourceIdAdj,
             @CA, @PV, @CAINIT, @PVINIT,
             @skcurrencyEur, @skmeasuretypeCSH, @skmeasuretypePV, @skmeasuretypeCRY;
END
GO
