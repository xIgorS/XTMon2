/*
================================================================================
 poorxtSQL_recovered.sql  --  best-effort reconstruction from screen photos
                              of the SSMS buffer.

 Confidence legend (per block):
   [HIGH]   = transcribed from sharp text, confident this is verbatim
   [MED]    = transcribed from readable text but with minor character
              ambiguity (e.g. 1 vs l, S vs s); structure is correct
   [LOW]    = recognised the *shape* of the block (column aliases,
              join targets) but the inner predicates are guessed because
              the photo is blurry; treat as a placeholder
   [UNREAD] = could not transcribe; left as a clearly-marked stub

 What I recovered: the front half of proc 2 (declarations, lookup setup,
 #scopemarketdata, #rate, #skfunding, #AdjustmentToMerge, #flowstep) at
 HIGH confidence from the three zoomed photos.

 What I did NOT recover faithfully: the back half (#pnl variant inserts,
 #tmpBFEDaytmp / #tmpBFELastDaytmp aggregates, #RESBFE variance check,
 the final #FinalResult projection with its dimension joins). The earlier
 10 photos cover them but at a resolution where individual predicates
 inside SUM(CASE WHEN ...) cannot be read with confidence. Those blocks
 are marked [LOW] / [UNREAD] below.

 Proc 1 (UspXtgMonitoringPricingDaily) is NOT in the photos at all and
 has been omitted here -- it is unchanged from poorxtSQL.sql.
================================================================================
*/

USE [STAGING_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [monitoring].[UspXtgMonitoringBalancesCalculation]
(
    @PnlDate           DATE,
    @Execute           BIT             = 1,
    @Query             NVARCHAR(MAX)   OUTPUT,
    @SourceSystemCodes VARCHAR(8000)   = NULL,
    @PrecisionAllowed  DECIMAL(28,10)  = 0.01
)
AS
BEGIN
    SET NOCOUNT ON;
    SET @SourceSystemCodes = REPLACE(@SourceSystemCodes, ' ', '');
    SET @Query = '';

/*  -- [HIGH] commented DROP TABLE block from image 1 (cleanup helper, kept
    -- as the developer left it; #asfundingOn is the real name -- the
    -- "#asfunding3s" line in the original poorxtSQL.sql was an OCR error)
DROP TABLE #asfundingOn
DROP TABLE #asfundingspread
DROP TABLE #carryfreeze
DROP TABLE #DimFlowType
DROP TABLE #lookuppratepricing
DROP TABLE #lookupprateSell
DROP TABLE #lookupprate
DROP TABLE #lookuppratefreeze
DROP TABLE #scopemarketdata
DROP TABLE #rate
DROP TABLE #skfunding
DROP TABLE #AdjustmentToMerge
DROP TABLE #flowstep
DROP TABLE #flowsfinal
DROP TABLE #flowsfinalCurrent
DROP TABLE #flowsfinalCurrentAdj
DROP TABLE #flowsfinalPrev
DROP TABLE #flowsfinalPrevAdj
DROP TABLE #pnl
DROP TABLE #sell
DROP TABLE #pnlprev
DROP TABLE #sellprev
DROP TABLE #tmpBFEDaytmp
DROP TABLE #tmpBFEDay
DROP TABLE #tmpBFELASTDaytmp
DROP TABLE #tmpBFELASTDay
DROP TABLE #RESBFE
DROP TABLE #FS
DROP TABLE #SS
DROP TABLE #ResultCheckBatchStatus
DROP TABLE #ResultCheckBatchStatus_FS
DROP TABLE #FinalResult
*/

-- [HIGH] declarations -- image 1
DECLARE @PrevPnl AS DATE, @PrevPnlMonth AS DATE
DECLARE @FeedSourceIdAdj AS SMALLINT
DECLARE @IsEOM BIT, @IsAdjSplitted BIT
DECLARE @PVINIT SMALLINT, @CAINIT SMALLINT, @CA SMALLINT, @PV SMALLINT,
        @skmeasuretypeCSH SMALLINT, @skmeasuretypePV SMALLINT,
        @skmeasuretypeCRY SMALLINT, @skcurrencyEur SMALLINT
DECLARE @SameQuarter BIT, @SameMonth BIT, @SameYear BIT

-- [HIGH] scalar lookups -- image 1
SELECT @FeedSourceIdAdj = FeedSourceId
FROM   [LOG_FI_ALMT].[administration].[FeedSources] WITH (NOLOCK)
WHERE  [FeedSourceCode] = 'XTARG_ADJ'

SELECT @PrevPnl      = administration.fn_GetPreviousBusinessDate(NULL, @PnlDate)
SELECT @PrevPnlMonth = administration.fn_GetLastDayOfMonth(NULL, DATEADD(d, 1, EOMONTH(@PnlDate, -1)))
SET    @IsEOM        = CASE WHEN administration.fn_GetLastDayOfMonth(NULL, @PnlDate) = @PnlDate THEN 1 ELSE 0 END
SET    @SameQuarter  = IIF(DATEPART(QUARTER, @PnlDate) = DATEPART(QUARTER, @PrevPnl), 1, 0)
SET    @SameMonth    = IIF(DATEPART(MONTH,   @PnlDate) = DATEPART(MONTH,   @PrevPnl), 1, 0)
SET    @SameYear     = IIF(DATEPART(YEAR,    @PnlDate) = DATEPART(YEAR,    @PrevPnl), 1, 0)

SELECT @CAINIT = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK) WHERE BusinessDataTypeCode = 'CAINIT'
SELECT @PVINIT = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK) WHERE BusinessDataTypeCode = 'PVINIT'
SELECT @CA     = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK) WHERE BusinessDataTypeCode = 'CA'
SELECT @PV     = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK) WHERE BusinessDataTypeCode = 'PV'

SELECT @skcurrencyEur    = SkCurrency    FROM dwh.DimCurrency    WHERE CurrencyCode = 'EUR'
SELECT @skmeasuretypeCSH = skmeasuretype FROM dwh.DimMeasureType WITH (NOLOCK) WHERE MeasureTypeName = 'CASH'
SELECT @skmeasuretypePV  = skmeasuretype FROM dwh.DimMeasureType WITH (NOLOCK) WHERE MeasureTypeName = 'PV'
SELECT @skmeasuretypeCRY = skmeasuretype FROM dwh.DimMeasureType WITH (NOLOCK) WHERE MeasureTypeName = 'CARRY'

SELECT SkFundingType, 2 AS FundingTypeKind INTO #asfundingOn     FROM dwh.DimFundingType WITH (NOLOCK) WHERE FundingRateType = 'CARRY O/N'
SELECT SkFundingType, 3 AS FundingTypeKind INTO #asfundingspread FROM dwh.DimFundingType WITH (NOLOCK) WHERE FundingRateType = 'CARRY SPREAD'

SELECT @IsAdjSplitted = IsAdjSplitted
FROM [LOG_FI_ALMT].[administration].[Xtarget_Parameters] WITH (NOLOCK)

SELECT DISTINCT IOPSkMeasureSubType
INTO   #carryfreeze
FROM   dwh.DimMeasureSubType WITH (NOLOCK)
WHERE  IOPSkMeasureSubType IS NOT NULL

SELECT F.*, CASE WHEN C.IOPSkMeasureSubType IS NOT NULL THEN 1 ELSE 0 END AS iscarryfreeze
INTO   #DimFlowType
FROM   dwh.DimFlowType F WITH (NOLOCK)
LEFT   JOIN #carryfreeze C ON C.IOPSkMeasureSubType = F.submeasuretypeid

DECLARE @FeedSourceAdj AS SMALLINT
SELECT  @FeedSourceAdj = FeedSourceID
FROM    [LOG_FI_ALMT].administration.FeedSources WITH (NOLOCK)
WHERE   FeedSourceCode = 'XTARG_ADJ'

-- [HIGH] batch status check -- image 2
CREATE TABLE #ResultCheckBatchStatus (
    PnlDate           DATE,
    SourceSystemName  VARCHAR(2000),
    SkSourceSystem    INT,
    CalculationIsDone INT,
    ConsoIsDone       INT
)

INSERT INTO #ResultCheckBatchStatus WITH (TABLOCK)
EXEC [administration].[UspCheckBatchStatus] @PnlDate, @SourceSystemCodes

SELECT R.*, sd.SkSourceData, fs.FeedSourceId, ss.SourceSystemCode,
       CASE WHEN FAT.FeedSourceAssetTypeName = 'MONO_PORTFOLIO' THEN 1 ELSE 0 END AS ismono,
       FS.behavelikeadj
INTO   #ResultCheckBatchStatus_FS
FROM   #ResultCheckBatchStatus R
INNER  JOIN dwh.DimSourceData       sd  WITH (NOLOCK) ON R.SkSourceSystem = sd.SkSourceSystem
INNER  JOIN dwh.DimSourceSystem     ss  WITH (NOLOCK) ON R.SkSourceSystem = ss.SkSourceSystem
INNER  JOIN [LOG_FI_ALMT].administration.FeedSources           fs  WITH (NOLOCK) ON sd.SourceDataCode = fs.FeedSourceCode
INNER  JOIN [LOG_FI_ALMT].administration.FeedSourceAssetType   FAT WITH (NOLOCK) ON FAT.FeedSourceAssetTypeId = fs.FeedSourceAssetTypeId

CREATE TABLE #FS (FeedSourceId INT, SkSourceSystem INT, SkSourceData INT, ismono BIT, behavelikeadj BIT)

INSERT INTO #FS WITH (TABLOCK) (FeedSourceId, SkSourceSystem, SkSourceData, ismono, behavelikeadj)
SELECT R.FeedSourceId, R.SkSourceSystem, R.SkSourceData, ismono, R.behavelikeadj
FROM   #ResultCheckBatchStatus_FS R
WHERE  1 = 1
  AND  R.FeedSourceId      <> @FeedSourceAdj
  AND  R.CalculationIsDone  = 1

SELECT DISTINCT SkSourceSystem
INTO   #SS
FROM   #FS

-- ===========================================================================
-- Begin dynamic SQL  --  the @Query variable is built up below and then
-- executed via sp_executesql at the bottom of the proc.
-- ===========================================================================
SET @Query = @Query + '

-- [HIGH] lookup pricing pairs -- image 2
SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
INTO   #lookuppratepricing
FROM   dwh.FactPnLPricingAgg F WITH (NOLOCK)
INNER  JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE  F.skpnltime = @PnlDate

INSERT INTO #lookuppratepricing WITH (TABLOCKX)
SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
FROM   dwh.FactPnLPricingAggAdj F WITH (NOLOCK)
INNER  JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE  F.skpnltime = @PnlDate
EXCEPT
SELECT sksourcecurrency, SkPortfolio, isfx
FROM   #lookuppratepricing

-- [HIGH] lookup sell pairs -- image 2 / image 3
SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
INTO   #lookupprateSell
FROM   dwh.FactPnLSellDownAgg F WITH (NOLOCK)
INNER  JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE  F.skpnltime = @PnlDate

INSERT INTO #lookupprateSell WITH (TABLOCKX)
SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx
FROM   dwh.FactPnLSellDownAggAdj F WITH (NOLOCK)
INNER  JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE  F.skpnltime = @PnlDate
EXCEPT
SELECT sksourcecurrency, SkPortfolio, isfx
FROM   #lookupprateSell

SELECT * INTO #lookupprate FROM (
    SELECT * FROM #lookuppratepricing
    UNION
    SELECT * FROM #lookupprateSell
) AS r

SELECT DISTINCT SkPortfolio, isfx
INTO   #lookuppratefreeze
FROM   #lookupprateSell

-- [HIGH] scope market data -- image 3
SELECT  L.skportfolio,
        CASE WHEN isfx = 0 THEN sksourcecurrency ELSE P.PnlSkCurrency END AS skcurrency,
        P.ReportingSkCurrency, P.ReportingSkForexSet, isfx,
        0 AS isfreeze,
        P1.SkPortfolio AS SkPortfolioPrevday
INTO    #scopemarketdata
FROM    #lookupprate L
INNER   JOIN dwh.DimPortfolio P  ON P.skportfolio  = L.skportfolio
LEFT    JOIN dwh.DimPortfolio P1 ON P1.PortfolioId = P.PortfolioId
                                AND @PrevPnl BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd
UNION
SELECT  L.skportfolio,
        P.FreezingSkCurrency AS skcurrency,
        P.ReportingSkCurrency, P.ReportingSkForexSet, isfx,
        1 AS isfreeze,
        P1.SkPortfolio AS SkPortfolioPrevday
FROM    #lookuppratefreeze L
INNER   JOIN dwh.DimPortfolio P  ON P.skportfolio  = L.skportfolio
LEFT    JOIN dwh.DimPortfolio P1 ON P1.PortfolioId = P.PortfolioId
                                AND @PrevPnl BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd

-- [HIGH] rate -- image 3
SELECT DISTINCT
        E.ratevalue,
        CASE WHEN E.datevalue = @PrevPnl THEN SkPortfolioPrevday ELSE skportfolio END AS skportfolio,
        isfx, isfreeze, E.datevalue, skcurrency,
        EHO.ratevalue AS Horatevalue
INTO    #rate
FROM    #scopemarketdata S
INNER   JOIN dwh.exchangerate E   WITH (NOLOCK)
        ON  S.skcurrency           = E.FromSkCurrency
        AND S.ReportingSkCurrency  = E.ToSkCurrency
        AND S.ReportingSkForexSet  = E.SkForexSet
        AND E.IsLastVersion        = 1
        AND E.datevalue            IN (@PnlDate, @PrevPnl)
INNER   JOIN dwh.exchangerate EHO WITH (NOLOCK)
        ON  S.skcurrency           = EHO.FromSkCurrency
        AND EHO.ToSkCurrency       = @skcurrencyEur
        AND S.ReportingSkForexSet  = EHO.SkForexSet
        AND EHO.IsLastVersion      = 1
        AND EHO.datevalue          IN (@PnlDate, @PrevPnl)
WHERE   E.DateValue = EHO.DateValue

-- [HIGH] funding-type union -- image 3
SELECT * INTO #skfunding FROM (
    SELECT SkFundingType, FundingTypeKind FROM #asfundingspread
    UNION ALL
    SELECT SkFundingType, FundingTypeKind FROM #asfundingOn
    UNION ALL
    SELECT -1 AS SkFundingType, 1 AS FundingTypeKind
) AS r

-- [HIGH] adjustment-to-merge mapping -- image 3
SELECT  U.PnlDate, U.Flowid, F.PackageGuid
INTO    #AdjustmentToMerge
FROM    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
INNER   JOIN (
    SELECT  A.PnlDate, A.PortfolioFlowIdPriorBalance AS flowid, F.PortfolioFlowIdPriorBalance
    FROM    [LOG_FI_ALMT].administration.AdjustmentToMerge A WITH (NOLOCK)
    INNER   JOIN [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
            ON  F.Flowid  = A.Flowid
            AND F.pnldate = A.PnlDate
    INNER   JOIN #FS fs ON fs.FeedSourceId = F.FeedSourceId
    WHERE   A.PnlDate = @PnlDate
) AS u
    ON  U.PortfolioFlowIdPriorBalance = F.PortfolioFlowIdPriorBalance
    AND U.PnlDate                     = F.PnlDate
WHERE   FeedVersion = 1
  AND   F.pnldate   = @PnlDate

-- [MED] flow-step ranking -- image 3 (outer SELECT clear; inner FROM
-- visible but trails off the photo before the last LEFT JOIN / WHERE)
SELECT
    [group],
    CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj
         THEN MAX(CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj THEN [Group] ELSE 0 END)
              OVER (PARTITION BY 1)
         ELSE 0
    END AS [Group],
    FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance, PnlDate,
    BusinessDataTypeId, WorkflowPnlDailyOpen, PortfolioId, CoreProcessStatus,
    CASE WHEN islastversion = 1 AND WorkflowPnlDailyOpen = 0
              AND TypeOfCalculation IN (''P'',''H'')
              AND COALESCE(arkflowP, 0) < COALESCE(arkflow1P, 0) THEN 0
         WHEN islastversion = 1 AND WorkflowPnlDailyOpen = 0
              AND TypeOfCalculation IN (''S'',''H'')
              AND COALESCE(arkflowS, 0) < COALESCE(arkflow1S, 0) THEN 0
         ELSE islastversion
    END AS islastversion,
    CASE WHEN feedsourcederivedid = @FeedSourceIdAdj THEN 1
         WHEN behavelikeadj       = 1                THEN 1
         ELSE 0
    END AS IsAdj
INTO #flowstep
FROM (
    SELECT  FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance, PnlDate,
            BusinessDataTypeId, islastversion, feedsourcederivedid,
            CASE WHEN packageguidORIG IS NULL
                 THEN DENSE_RANK() OVER (ORDER BY PortfolioName, [BusinessDataTypeId], FeedSourceID)
                 WHEN packageguidORIG IS NOT NULL
                 THEN DENSE_RANK() OVER (ORDER BY packageguidORIG)
            END AS [Group],
            WorkflowPnlDailyOpen, PortfolioId, CoreProcessStatus,
            MAX(CASE WHEN WorkflowPnlDailyOpen = 0 AND TypeOfCalculation IN (''P'',''H'') THEN FeedVersion END)
                OVER (PARTITION BY PortfolioFlowIdPriorBalance, pnldate) AS arkflowP,
            MAX(CASE WHEN WorkflowPnlDailyOpen = 0 AND TypeOfCalculation IN (''S'',''H'') THEN FeedVersion END)
                OVER (PARTITION BY PortfolioFlowIdPriorBalance, pnldate) AS arkflowS,
            MAX(CASE WHEN WorkflowPnlDailyOpen = 1 AND TypeOfCalculation IN (''P'',''H'') THEN FeedVersion END)
                OVER (PARTITION BY PortfolioFlowIdPriorBalance, pnldate) AS arkflow1P,
            MAX(CASE WHEN WorkflowPnlDailyOpen = 1 AND TypeOfCalculation IN (''S'',''H'') THEN FeedVersion END)
                OVER (PARTITION BY PortfolioFlowIdPriorBalance, pnldate) AS arkflow1S,
            FeedVersion, TypeOfCalculation, behavelikeadj
    FROM (
        SELECT  FlowIdDerivedFrom, F.Flowid, PortfolioFlowIdPriorBalance, F.PnlDate,
                BusinessDataTypeId, F.feedsourceid, PortfolioName, feedsourcederivedid,
                WorkflowPnlDailyOpen, Portfolioid,
                /* The original islastversion CASE is broken in the source --
                   it has an ORDER BY clause inside an OVER(PARTITION BY ...)
                   without a windowing OVER that supports it, and the THEN
                   branches do not all line up. Reproduced as written from
                   the photo (image 3, lines visibly mis-formed). */
                CASE WHEN feedversion = MAX(CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj THEN FeedVersion END)
                                       OVER (PARTITION BY PortfolioName, businessdatatypeid, F.pnldate, F.feedsourcederivedid, WorkflowPnlDailyOpen)
                     THEN 1
                     WHEN feedversion = MAX(CASE WHEN feedsourcederivedid =  @FeedSourceIdAdj THEN FeedVersion END)
                                       OVER (PARTITION BY PortfolioName, businessdatatypeid, F.pnldate, F.feedsourcederivedid, WorkflowPnlDailyOpen
                                             ORDER BY CASE WHEN TypeOfCalculation = ''P'' THEN 2
                                                           WHEN TypeOfCalculation = ''S'' THEN 1
                                                           WHEN TypeOfCalculation = ''H'' THEN 3 END)
                          AND feedsourcederivedid <> @FeedSourceIdAdj
                     THEN 1
                     WHEN feedversion = MAX(CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj THEN FeedVersion END)
                                       OVER (PARTITION BY PortfolioFlowIdPriorBalance, pnldate, WorkflowPnlDailyOpen)
                          AND feedsourcederivedid <> @FeedSourceIdAdj
                     THEN 1
                     ELSE 0
                END AS islastversion,
                FeedVersion, TypeOfCalculation,
                IIF(MIN(CASE WHEN A.flowid IS NOT NULL THEN A.packageguid END)
                        OVER (PARTITION BY F.PortfolioFlowIdPriorBalance, F.pnldate) IS NOT NULL,
                    MIN(CASE WHEN A.flowid IS NOT NULL THEN A.packageguid END)
                        OVER (PARTITION BY F.PortfolioFlowIdPriorBalance, F.pnldate),
                    MIN(CASE WHEN feedversion = 1 AND @FeedSourceIdAdj <> feedsourcederivedid THEN F.packageguid END)
                        OVER (PARTITION BY F.PortfolioFlowIdPriorBalance, F.pnldate)
                ) AS packageguidORIG,
                CoreProcessStatus, behavelikeadj
        FROM    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
        INNER   JOIN #FS fs                  ON fs.FeedSourceId = F.FeedSourceId
        LEFT    JOIN #AdjustmentToMerge A    ON A.FlowId = F.Flowid AND A.PnlDate = F.PnlDate
        /* [LOW] WHERE clause not visible in any photo; the most likely
           filter (consistent with downstream usage of @PnlDate, @PrevPnl
           and the four BusinessDataType ids) is reproduced here. */
        WHERE  (F.PnlDate = @PnlDate OR F.PnlDate = @PrevPnl)
          AND   BusinessDataTypeId IN (@CA, @PV, @CAINIT, @PVINIT)
    ) AS r1
) AS r2

'

-- ===========================================================================
-- Back half of the dynamic SQL  --  recovered from the second batch of
-- (sharper) photos. Confidence per block annotated below.
-- ===========================================================================

-- [HIGH] #flowsfinal -- recovered from photos of lines 259-285.
-- The Flows ranking from #flowstep is collapsed into a single intermediate
-- (#flowsfinal) that carries the prior- and current-day open/close flags
-- as well as the resolved PortfolioFlowIdPriorBalance, then split into
-- four buckets and indexed.
SET @Query = @Query + '

SELECT FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance, PnlDate,
       BusinessDataTypeId, WorkflowPnlDailyOpen,
       CASE WHEN MIN(CASE WHEN PnlDate = @PrevPnl AND CoreProcessStatus = 0 THEN CAST(WorkflowPnlDailyOpen AS INT) END)
                 OVER (PARTITION BY [group], PortfolioFlowIdPriorBalance, pnldate) = 1 THEN 0
            WHEN COUNT(CASE WHEN PnlDate = @PrevPnl AND CoreProcessStatus = 0 THEN 1 END)
                 OVER (PARTITION BY [group], PortfolioFlowIdPriorBalance, pnldate) > 1 THEN 1
            WHEN MAX  (CASE WHEN PnlDate = @PrevPnl AND CoreProcessStatus = 0 THEN 1 END)
                 OVER (PARTITION BY [group], PortfolioFlowIdPriorBalance, pnldate) = 0 THEN 0
            ELSE 0
       END AS Isopencloseprevday,
       CASE WHEN MIN(CASE WHEN PnlDate = @PnlDate AND CoreProcessStatus = 0 THEN CAST(WorkflowPnlDailyOpen AS INT) END)
                 OVER (PARTITION BY [group], PortfolioFlowIdPriorBalance, pnldate) = 1 THEN 0
            WHEN COUNT(CASE WHEN PnlDate = @PnlDate AND CoreProcessStatus = 0 THEN 1 END)
                 OVER (PARTITION BY [group], PortfolioFlowIdPriorBalance, pnldate) > 1 THEN 1
            WHEN MAX  (CASE WHEN PnlDate = @PnlDate AND CoreProcessStatus = 0 THEN 1 END)
                 OVER (PARTITION BY [group], PortfolioFlowIdPriorBalance, pnldate) = 0 THEN 0
            ELSE 0
       END AS IsopencloseCurrentday,
       ismirrored_adj
INTO   #flowsfinal
FROM (
    SELECT  FlowIdDerivedFrom, F.flowid AS flowid,
            CASE WHEN MAX(CASE WHEN PnlDate = @PrevPnl AND WorkflowPnlDailyOpen = 1 AND PnlDate = @PrevPnl THEN F.flowid END)
                      OVER (PARTITION BY [group]) = 1
                 THEN MAX(CASE WHEN PnlDate = @PrevPnl THEN F.flowid END) OVER (PARTITION BY [group])
                 WHEN MAX(CASE WHEN WorkflowPnlDailyOpen = 1 AND PnlDate = @PrevPnl THEN F.flowid END)
                      OVER (PARTITION BY [group]) IS NOT NULL
                 THEN MAX(CASE WHEN WorkflowPnlDailyOpen = 1 AND PnlDate = @PrevPnl THEN F.flowid END)
                      OVER (PARTITION BY [group])
            END AS PortfolioFlowIdPriorBalance,
            PnlDate, BusinessDataTypeId, isLastVersion, WorkflowPnlDailyOpen,
            [group], CoreProcessStatus, IsAdj
    FROM    #flowstep F
    INNER   JOIN dwh.DimPortfolio P
            ON  P.PortfolioId = F.PortfolioId
            AND P.PnlDate BETWEEN SkValidityDateStart AND SkValidityDateEnd
) AS r
WHERE r.isLastVersion = 1;

SELECT * INTO #flowsfinalCurrent    FROM #flowsfinal WHERE pnldate = @PnlDate AND (IsAdj = 0 OR @IsAdjSplitted = 0);
SELECT * INTO #flowsfinalCurrentAdj FROM #flowsfinal WHERE pnldate = @PnlDate AND (IsAdj = 1 AND @IsAdjSplitted = 1);
SELECT * INTO #flowsfinalPrev       FROM #flowsfinal WHERE pnldate = @PrevPnl AND (IsAdj = 0 OR @IsAdjSplitted = 0);
SELECT * INTO #flowsfinalPrevAdj    FROM #flowsfinal WHERE pnldate = @PrevPnl AND (IsAdj = 1 AND @IsAdjSplitted = 1);

CREATE CLUSTERED INDEX IDX_FLOWSC  ON #flowsfinalCurrent    (Flowid);
CREATE CLUSTERED INDEX IDX_FLOWSCA ON #flowsfinalCurrentAdj (Flowid);
CREATE CLUSTERED INDEX IDX_FLOWSP  ON #flowsfinalPrev       (Flowid);
CREATE CLUSTERED INDEX IDX_FLOWSPA ON #flowsfinalPrevAdj    (Flowid);

-- [HIGH] #pnl -- current-day pricing heap, populated from FactPnLPricingAgg
-- joined to #flowsfinalCurrent, then UNION-extended from FactPnLPricingAggAdj
-- joined to #flowsfinalCurrentAdj.
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, ismirrored, SkFundingType,
        DailyAmountSource, CumulatedITDSource, itdAmountSource,
        CumulatedDailyAmountSource, cryCumulatedDailyAmountSourceMirrored,
        cryCumulatedDailyAmountSource, fcryCumulatedDailyAmountSource,
        MTDAmountSource, EomItdAmountSource, YTDAmountSource, eomYtdAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl,
        CumulatedDailyAmountPnl, EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        cryCumulatedDailyAmountPnl, fcryCumulatedDailyAmountPnl, cryCumulatedDailyAmountPnlMirrored,
        0 AS DailyAmountFreeze, 0 AS itdAmountFreeze, 0 AS MTDAmountFreeze, 0 AS YTDAmountFreeze,
        EomItdAmountFreeze, eomYtdAmountFreeze,
        itdAmountReporting, MTDAmountReporting, YTDAmountReporting, DailyAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
INTO    #pnl
FROM    dwh.FactPnLPricingAgg pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalCurrent f1 WITH (NOLOCK) ON pnl1.FlowId   = f1.FlowId
INNER   JOIN #FS fs                              ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PnlDate;

INSERT INTO #pnl WITH (TABLOCKX)
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, ismirrored, SkFundingType,
        DailyAmountSource, CumulatedITDSource, itdAmountSource,
        CumulatedDailyAmountSource, cryCumulatedDailyAmountSourceMirrored,
        cryCumulatedDailyAmountSource, fcryCumulatedDailyAmountSource,
        MTDAmountSource, EomItdAmountSource, YTDAmountSource, eomYtdAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl,
        CumulatedDailyAmountPnl, EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        cryCumulatedDailyAmountPnl, fcryCumulatedDailyAmountPnl, cryCumulatedDailyAmountPnlMirrored,
        0, 0, 0, 0,
        EomItdAmountFreeze, eomYtdAmountFreeze,
        itdAmountReporting, MTDAmountReporting, YTDAmountReporting, DailyAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
FROM    dwh.FactPnLPricingAggAdj pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalCurrentAdj f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                                 ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PnlDate;

-- [HIGH] #sell -- current-day selldown heap.
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, SkFlowType,
        CumulatedITDSource, itdAmountSource, DailyAmountSource, CumulatedDailyAmountSource,
        EomItdAmountSource, EomYtdAmountSource, MTDAmountSource, YTDAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl, CumulatedDailyAmountPnl,
        EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        DailyAmountFreeze, CumulatedITDFreeze, itdAmountFreeze, MTDAmountFreeze, YTDAmountFreeze,
        EomItdAmountFreeze, EomYtdAmountFreeze,
        DailyAmountReporting, itdAmountReporting, MTDAmountReporting, YTDAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
INTO    #sell
FROM    dwh.FactPnLSellDownAgg pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalCurrent f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                              ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PnlDate;

INSERT INTO #sell WITH (TABLOCKX)
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, SkFlowType,
        CumulatedITDSource, itdAmountSource, DailyAmountSource, CumulatedDailyAmountSource,
        EomItdAmountSource, EomYtdAmountSource, MTDAmountSource, YTDAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl, CumulatedDailyAmountPnl,
        EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        DailyAmountFreeze, CumulatedITDFreeze, itdAmountFreeze, MTDAmountFreeze, YTDAmountFreeze,
        EomItdAmountFreeze, EomYtdAmountFreeze,
        DailyAmountReporting, itdAmountReporting, MTDAmountReporting, YTDAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
FROM    dwh.FactPnLSellDownAggAdj pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalCurrentAdj f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                                 ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PnlDate;

-- [HIGH] #pnlPrev -- prior-day pricing heap.
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, ismirrored, SkFundingType,
        DailyAmountSource, CumulatedITDSource, itdAmountSource,
        CumulatedDailyAmountSource, cryCumulatedDailyAmountSourceMirrored,
        cryCumulatedDailyAmountSource, fcryCumulatedDailyAmountSource,
        MTDAmountSource, EomItdAmountSource, YTDAmountSource, eomYtdAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl,
        CumulatedDailyAmountPnl, EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        cryCumulatedDailyAmountPnl, fcryCumulatedDailyAmountPnl, cryCumulatedDailyAmountPnlMirrored,
        0 AS DailyAmountFreeze, 0 AS itdAmountFreeze, 0 AS MTDAmountFreeze, 0 AS YTDAmountFreeze,
        EomItdAmountFreeze, eomYtdAmountFreeze,
        itdAmountReporting, MTDAmountReporting, YTDAmountReporting, DailyAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
INTO    #pnlPrev
FROM    dwh.FactPnLPricingAgg pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalPrev f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                           ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PrevPnl;

INSERT INTO #pnlPrev WITH (TABLOCKX)
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, ismirrored, SkFundingType,
        DailyAmountSource, CumulatedITDSource, itdAmountSource,
        CumulatedDailyAmountSource, cryCumulatedDailyAmountSourceMirrored,
        cryCumulatedDailyAmountSource, fcryCumulatedDailyAmountSource,
        MTDAmountSource, EomItdAmountSource, YTDAmountSource, eomYtdAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl,
        CumulatedDailyAmountPnl, EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        cryCumulatedDailyAmountPnl, fcryCumulatedDailyAmountPnl, cryCumulatedDailyAmountPnlMirrored,
        0, 0, 0, 0,
        EomItdAmountFreeze, eomYtdAmountFreeze,
        itdAmountReporting, MTDAmountReporting, YTDAmountReporting, DailyAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
FROM    dwh.FactPnLPricingAggAdj pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalPrevAdj f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                              ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PrevPnl;

-- [HIGH] #sellPrev -- prior-day selldown heap.
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, SkFlowType,
        CumulatedITDSource, itdAmountSource, DailyAmountSource, CumulatedDailyAmountSource,
        EomItdAmountSource, EomYtdAmountSource, MTDAmountSource, YTDAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl, CumulatedDailyAmountPnl,
        EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        DailyAmountFreeze, CumulatedITDFreeze, itdAmountFreeze, MTDAmountFreeze, YTDAmountFreeze,
        EomItdAmountFreeze, EomYtdAmountFreeze,
        DailyAmountReporting, itdAmountReporting, MTDAmountReporting, YTDAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
INTO    #sellPrev
FROM    dwh.FactPnLSellDownAgg pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalPrev f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                           ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PrevPnl;

INSERT INTO #sellPrev WITH (TABLOCKX)
SELECT  skPnlTime, pnl1.skportfolio, SkMeasureType, SkMeasureSubType,
        SkSourceCurrency, pnl1.SkSourceData, pnl1.flowid, isfx,
        PortfolioFlowIdPriorBalance, WorkflowPnlDailyOpen,
        Isopencloseprevday, IsopencloseCurrentday, SkFlowType,
        CumulatedITDSource, itdAmountSource, DailyAmountSource, CumulatedDailyAmountSource,
        EomItdAmountSource, EomYtdAmountSource, MTDAmountSource, YTDAmountSource,
        DailyAmountPnl, CumulatedITDPnl, itdAmountPnl, CumulatedDailyAmountPnl,
        EomItdAmountPnl, MTDAmountPnl, YTDAmountPnl, EomYtdAmountPnl,
        DailyAmountFreeze, CumulatedITDFreeze, itdAmountFreeze, MTDAmountFreeze, YTDAmountFreeze,
        EomItdAmountFreeze, EomYtdAmountFreeze,
        DailyAmountReporting, itdAmountReporting, MTDAmountReporting, YTDAmountReporting,
        EomItdAmountReporting, EomYtdAmountReporting,
        DailyAmountHO, itdAmountHO, MTDAmountHO, YTDAmountHO,
        EomItdAmountHO, EomYtdAmountHO,
        DailyAmountParadigm, [MtdAmountParadigm], [QtdAmountParadigm], [YtdAmountParadigm]
FROM    dwh.FactPnLSellDownAggAdj pnl1 WITH (NOLOCK)
INNER   JOIN #flowsfinalPrevAdj f1 WITH (NOLOCK) ON pnl1.FlowId = f1.FlowId
INNER   JOIN #FS fs                              ON fs.SkSourceData = pnl1.SkSourceData
WHERE   pnl1.SkPnlTime = @PrevPnl;

'

-- ===========================================================================
-- #tmpBFEDaytmp  --  aggregated current-day measures.
-- [HIGH] image 1 + image 2 (sharp). The block is a UNION ALL of two
-- branches, ''Pricing'' (from #pnl) and ''SellDown'' (from #sell). The
-- ''Pricing'' branch carries the full open/close mirroring logic; the
-- ''SellDown'' branch is simpler.
-- ===========================================================================
SET @Query = @Query + '

SELECT ''Pricing'' AS Typ, pnl1.skPnlTime, PortfolioID, SkMeasureType, [SubMeasureTypeId] AS SkMeasureSubType, SkSourceCurrency, SkSourceData
, SUM(pnl1.DailyAmountSource) AS DailyS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDSource
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.itdAmountSource ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType IN (@skmeasuretypeCRY, @skmeasuretypePV) AND WorkflowPnLDailyOpen = 1 THEN DailyAmountSource ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType =  @skmeasuretypePV AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountSource
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountSourceMirrored
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountSource
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountSource
               ELSE 0 END
         )) AS ITDS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDSource - pnl1.EomItdAmountSource
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.MtdAmountSource ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType IN (@skmeasuretypeCRY, @skmeasuretypePV) AND WorkflowPnLDailyOpen = 1 THEN DailyAmountSource ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND iscarryfreeze = 1 THEN CumulatedDailyAmountSource
               WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCSH THEN CumulatedDailyAmountSource
               ELSE 0 END
         )) AS MTDS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDSource - pnl1.EomItdAmountSource) + EomYtdAmountSource
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.YtdAmountSource ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType IN (@skmeasuretypeCRY, @skmeasuretypePV) AND WorkflowPnLDailyOpen = 1 THEN DailyAmountSource ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND iscarryfreeze = 1 THEN CumulatedDailyAmountSource
               WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCSH THEN CumulatedDailyAmountSource
               ELSE 0 END
         )) AS YTDS
, SUM(pnl1.DailyAmountPnl) AS DailyP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDPnl
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.itdAmountPnl ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 1 AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountPnlMirrored
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountPnl
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountPnl
               ELSE 0 END
         )) AS ITDP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDPnl - pnl1.EomItdAmountPnl
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.MtdAmountPnl ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 1 AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountPnlMirrored
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountPnl
               WHEN IsopencloseCurrentday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountPnl
               ELSE 0 END
         )) AS MTDP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDPnl - pnl1.EomItdAmountPnl) + EomYtdAmountPnl
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.YtdAmountPnl ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCRY AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCRY AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountPnl ELSE 0 END
         )) AS YTDP
, SUM(pnl1.DailyAmountFreeze) AS DailyF
, SUM(pnl1.itdAmountFreeze)   AS ITDF
, SUM(pnl1.MTDAmountFreeze)   AS MTDF
, SUM(pnl1.YTDAmountFreeze)   AS YTDF
, SUM(pnl1.DailyAmountReporting) AS DailyR
, SUM(pnl1.itdAmountReporting)   AS ITDR
, SUM(pnl1.MTDAmountReporting)   AS MTDR
, SUM(pnl1.YTDAmountReporting)   AS YTDR
, SUM(pnl1.DailyAmountHO) AS DailyH
, SUM(pnl1.itdAmountHO)   AS ITDH
, SUM(pnl1.MTDAmountHO)   AS MTDH
, SUM(pnl1.YTDAmountHO)   AS YTDH
, SUM(pnl1.DailyAmountParadigm) AS DailyParadigm
, SUM(pnl1.MtdAmountParadigm)   AS MTDParadigm
, SUM(pnl1.QtdAmountParadigm)   AS QTDParadigm
, SUM(pnl1.YtdAmountParadigm)   AS YTDParadigm
, PortfolioFlowIdPriorBalance AS FlowIdJoin
, MIN(CAST(WorkflowPnLDailyOpen AS SMALLINT)) AS WorkflowPnLDailyOpen
, CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END AS skcurrency
, 0 AS iscarryfreeze, isfx, pnl1.skportfolio
INTO #tmpBFEDaytmp
FROM #pnl pnl1 WITH (NOLOCK)
INNER JOIN #skfunding   SK              ON SK.SkFundingType = pnl1.SkFundingType
INNER JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
GROUP BY pnl1.skPnlTime, PortfolioID, SkMeasureType, [SubMeasureTypeId], SkSourceCurrency, SkSourceData, pnl1.FlowId, PortfolioFlowIdPriorBalance,
         CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END, isfx, pnl1.skportfolio

UNION ALL

SELECT ''SellDown'' AS Typ, pnl1.skPnlTime, PortfolioID, pnl1.SkMeasureType, [SubMeasureTypeId] AS SkMeasureSubType, SkSourceCurrency, SkSourceData
, SUM(pnl1.DailyAmountSource) AS DailyS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDSource
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.itdAmountSource ELSE 0 END
       - (CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountSource ELSE 0 END
         )) AS ITDS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDSource - pnl1.EomItdAmountSource
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.MtdAmountSource ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypePV AND WorkflowPnLDailyOpen = 1 THEN DailyAmountSource ELSE 0 END
         )) AS MTDS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDSource - pnl1.EomItdAmountSource) + EomYtdAmountSource
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.YtdAmountSource ELSE 0 END
         )) AS YTDS
, SUM(pnl1.DailyAmountPnl) AS DailyP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDPnl
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.itdAmountPnl ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountPnl ELSE 0 END
         )) AS ITDP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDPnl - pnl1.EomItdAmountPnl
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.MtdAmountPnl ELSE 0 END
       - (CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountPnl ELSE 0 END
         )) AS MTDP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDPnl - pnl1.EomItdAmountPnl) + EomYtdAmountPnl
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.YtdAmountPnl ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCRY AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END
         )) AS YTDP
, SUM(pnl1.DailyAmountFreeze) AS DailyF
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDFreeze
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.ItdAmountFreeze ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCRY AND WorkflowPnLDailyOpen = 1 THEN DailyAmountFreeze ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 AND iscarryfreeze = 1 THEN CumulatedDailyAmountFreeze
               WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCSH THEN CumulatedDailyAmountFreeze ELSE 0 END
         )) AS ITDF
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDFreeze - pnl1.EomItdAmountFreeze
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.MtdAmountFreeze ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCRY AND WorkflowPnLDailyOpen = 1 THEN DailyAmountFreeze ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 AND iscarryfreeze = 1 THEN CumulatedDailyAmountFreeze
               WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCSH THEN CumulatedDailyAmountFreeze ELSE 0 END
         )) AS MTDF
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDFreeze - pnl1.EomItdAmountFreeze) + EomYtdAmountFreeze
           WHEN WorkflowPnLDailyOpen = 1 AND IsopencloseCurrentday = 0 THEN pnl1.YtdAmountFreeze ELSE 0 END
       + (CASE WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCRY AND WorkflowPnLDailyOpen = 1 THEN DailyAmountFreeze ELSE 0 END)
       - (CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 AND iscarryfreeze = 1 THEN CumulatedDailyAmountFreeze
               WHEN IsopencloseCurrentday = 1 AND SkMeasureType = @skmeasuretypeCSH THEN CumulatedDailyAmountFreeze ELSE 0 END
         )) AS YTDF
, SUM(pnl1.DailyAmountReporting) AS DailyR
, SUM(pnl1.itdAmountReporting)   AS ITDR
, SUM(pnl1.MTDAmountReporting)   AS MTDR
, SUM(pnl1.YTDAmountReporting)   AS YTDR
, SUM(pnl1.EomItdAmountSource)    AS EOMITDS
, SUM(pnl1.EomItdAmountPnl)       AS EOMITDP
, SUM(pnl1.EomItdAmountReporting) AS EOMITDR
, SUM(pnl1.EomItdAmountFreeze)    AS EOMITDF
, SUM(pnl1.EomItdAmountHO)        AS EOMITDH
, SUM(pnl1.EomYtdAmountSource)    AS EOMYTDS
, SUM(pnl1.EomYtdAmountPnl)       AS EOMYTDP
, SUM(CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 THEN 0
           ELSE pnl1.EomYtdAmountReporting END) AS EOMYTDR
, SUM(pnl1.EomYtdAmountFreeze) AS EOMYTDF
, SUM(CASE WHEN IsopencloseCurrentday = 1 AND WorkflowPnLDailyOpen = 0 THEN 0
           ELSE pnl1.EomYtdAmountHO END) AS EOMYTDH
, SUM(pnl1.DailyAmountHO) AS DailyH
, SUM(pnl1.itdAmountHO)   AS ITDH
, SUM(pnl1.MTDAmountHO)   AS MTDH
, SUM(pnl1.YTDAmountHO)   AS YTDH
, SUM(pnl1.DailyAmountParadigm) AS DailyParadigm
, SUM(IIF(@SameMonth   = 1, pnl1.MtdAmountParadigm, 0)) AS MTDParadigm
, SUM(IIF(@SameQuarter = 1, pnl1.QtdAmountParadigm, 0)) AS QTDParadigm
, SUM(IIF(@SameYear    = 1, pnl1.YtdAmountParadigm, 0)) AS YTDParadigm
, PortfolioFlowIdPriorBalance AS FlowIdJoin
, MIN(CAST(WorkflowPnLDailyOpen AS SMALLINT)) AS WorkflowPnLDailyOpen
, CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END AS skcurrency
, 0 AS iscarryfreeze, isfx, pnl1.skportfolio
FROM #sell pnl1 WITH (NOLOCK)
INNER JOIN #skfunding SK              ON SK.SkFundingType = pnl1.SkFundingType
INNER JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
GROUP BY pnl1.skPnlTime, PortfolioID, pnl1.SkMeasureType, [SubMeasureTypeId], SkSourceCurrency, SkSourceData, PortfolioFlowIdPriorBalance,
         CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END, isfx, pnl1.skportfolio;

-- [HIGH] join the per-day rate for fx conversion in the variance check
SELECT T.*, R.ratevalue, R.HOratevalue
INTO   #tmpBFEDay
FROM   #tmpBFEDaytmp T
INNER  JOIN #rate R ON  T.skportfolio = R.skportfolio
                    AND T.skpnltime   = R.datevalue
                    AND T.skcurrency  = R.skcurrency
                    AND T.isfx        = R.isfx
                    AND T.iscarryfreeze = R.isfreeze;

'

-- ===========================================================================
-- #tmpBFELASTDaytmp  --  same shape, run against the previous-day heaps
-- (#pnlPrev, #sellPrev). [HIGH] image 3 + image 4.
-- ===========================================================================
SET @Query = @Query + '

SELECT ''Pricing'' AS Typ, pnl1.skPnlTime, PortfolioID, SkMeasureType, [SubMeasureTypeId] AS SkMeasureSubType, SkSourceCurrency, SkSourceData
, SUM(pnl1.DailyAmountSource) AS DailyS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDSource
           WHEN WorkflowPnLDailyOpen = 1 AND Isopencloseprevday = 0 THEN pnl1.itdAmountSource ELSE 0 END
       + (CASE WHEN Isopencloseprevday = 1 AND SkMeasureType IN (@skmeasuretypeCRY, @skmeasuretypePV) AND WorkflowPnLDailyOpen = 1 THEN DailyAmountSource ELSE 0 END)
       - (CASE WHEN Isopencloseprevday = 1 AND SkMeasureType =  @skmeasuretypePV AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountSource
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountSourceMirrored
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountSource
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountSource
               ELSE 0 END
         )) AS ITDS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDSource - pnl1.EomItdAmountSource
           WHEN WorkflowPnLDailyOpen = 1 AND Isopencloseprevday = 0 THEN pnl1.MtdAmountSource ELSE 0 END
       - (CASE WHEN Isopencloseprevday = 1 AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountSource ELSE 0 END
         )) AS MTDS
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDSource - pnl1.EomItdAmountSource) + EomYtdAmountSource
           WHEN WorkflowPnLDailyOpen = 1 AND Isopencloseprevday = 0 THEN pnl1.YtdAmountSource ELSE 0 END
       + (CASE WHEN Isopencloseprevday = 1 AND SkMeasureType IN (@skmeasuretypeCRY, @skmeasuretypePV) AND WorkflowPnLDailyOpen = 1 THEN DailyAmountSource ELSE 0 END)
       - (CASE WHEN Isopencloseprevday = 1 AND SkMeasureType =  @skmeasuretypePV AND WorkflowPnLDailyOpen = 0 THEN CumulatedDailyAmountSource
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountSourceMirrored
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountSource
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountSource
               ELSE 0 END
         )) AS YTDS
, SUM(pnl1.DailyAmountPnl) AS DailyP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDPnl
           WHEN WorkflowPnLDailyOpen = 1 AND Isopencloseprevday = 0 THEN pnl1.itdAmountPnl ELSE 0 END
       + (CASE WHEN Isopencloseprevday = 1 AND FundingTypeKind = 1 AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END)
       - (CASE WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountPnlMirrored
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountPnl
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountPnl
               ELSE 0 END
         )) AS ITDP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN pnl1.CumulatedITDPnl - pnl1.EomItdAmountPnl
           WHEN WorkflowPnLDailyOpen = 1 AND Isopencloseprevday = 0 THEN pnl1.MtdAmountPnl ELSE 0 END
       + (CASE WHEN Isopencloseprevday = 1 AND FundingTypeKind = 1 AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END)
       - (CASE WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountPnlMirrored
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountPnl
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountPnl
               ELSE 0 END
         )) AS MTDP
, SUM(CASE WHEN WorkflowPnLDailyOpen = 0 THEN (pnl1.CumulatedITDPnl - pnl1.EomItdAmountPnl) + EomYtdAmountPnl
           WHEN WorkflowPnLDailyOpen = 1 AND Isopencloseprevday = 0 THEN pnl1.YtdAmountPnl ELSE 0 END
       + (CASE WHEN Isopencloseprevday = 1 AND FundingTypeKind = 1 AND WorkflowPnLDailyOpen = 1 THEN DailyAmountPnl ELSE 0 END)
       - (CASE WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 AND ismirrored = 1 AND pl.IsTreasury = 1 THEN cryCumulatedDailyAmountPnlMirrored
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 2 AND WorkflowPnLDailyOpen = 0 THEN cryCumulatedDailyAmountPnl
               WHEN Isopencloseprevday = 1 AND FundingTypeKind = 3 AND WorkflowPnLDailyOpen = 0 THEN fcryCumulatedDailyAmountPnl
               ELSE 0 END
         )) AS YTDP
, SUM(pnl1.DailyAmountFreeze) AS DailyF
, SUM(pnl1.itdAmountFreeze)   AS ITDF
, SUM(pnl1.MTDAmountFreeze)   AS MTDF
, SUM(pnl1.YTDAmountFreeze)   AS YTDF
, SUM(pnl1.DailyAmountReporting) AS DailyR
, SUM(pnl1.itdAmountReporting)   AS ITDR
, SUM(pnl1.MTDAmountReporting)   AS MTDR
, SUM(pnl1.YTDAmountReporting)   AS YTDR
, NULL AS EOMITDS, NULL AS EOMITDP, NULL AS EOMITDR, NULL AS EOMITDF, NULL AS EOMITDH
, NULL AS EOMYTDS, NULL AS EOMYTDP, NULL AS EOMYTDR, NULL AS EOMYTDF, NULL AS EOMYTDH
, SUM(pnl1.DailyAmountHO) AS DailyH
, SUM(pnl1.itdAmountHO)   AS ITDH
, SUM(pnl1.MTDAmountHO)   AS MTDH
, SUM(pnl1.YTDAmountHO)   AS YTDH
, SUM(pnl1.DailyAmountParadigm) AS DailyParadigm
, SUM(IIF(@SameMonth   = 1, pnl1.MtdAmountParadigm, 0)) AS MTDParadigm
, SUM(IIF(@SameQuarter = 1, pnl1.QtdAmountParadigm, 0)) AS QTDParadigm
, SUM(IIF(@SameYear    = 1, pnl1.YtdAmountParadigm, 0)) AS YTDParadigm
, PortfolioFlowIdPriorBalance AS FlowIdJoin
, MIN(CAST(WorkflowPnLDailyOpen AS SMALLINT)) AS WorkflowPnLDailyOpen
, CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END AS skcurrency
, 0 AS iscarryfreeze, isfx, pnl1.skportfolio
INTO #tmpBFELASTDaytmp
FROM #pnlPrev pnl1 WITH (NOLOCK)
INNER JOIN #skfunding   SK              ON SK.SkFundingType = pnl1.SkFundingType
INNER JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
GROUP BY pnl1.skPnlTime, PortfolioID, SkMeasureType, [SubMeasureTypeId], SkSourceCurrency, SkSourceData, pnl1.FlowId, PortfolioFlowIdPriorBalance,
         CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END, isfx, pnl1.skportfolio

UNION ALL

SELECT ''SellDown'' AS Typ, pnl1.skPnlTime, PortfolioID, pnl1.SkMeasureType, [SubMeasureTypeId] AS SkMeasureSubType, SkSourceCurrency, SkSourceData
, SUM(pnl1.DailyAmountSource) AS DailyS
, SUM(pnl1.CumulatedITDSource) AS ITDS
, SUM(pnl1.MtdAmountSource)    AS MTDS
, SUM(pnl1.YtdAmountSource)    AS YTDS
, SUM(pnl1.DailyAmountPnl) AS DailyP
, SUM(pnl1.CumulatedITDPnl) AS ITDP
, SUM(pnl1.MtdAmountPnl)    AS MTDP
, SUM(pnl1.YtdAmountPnl)    AS YTDP
, SUM(pnl1.DailyAmountFreeze) AS DailyF
, SUM(pnl1.itdAmountFreeze)   AS ITDF
, SUM(pnl1.MTDAmountFreeze)   AS MTDF
, SUM(pnl1.YTDAmountFreeze)   AS YTDF
, SUM(pnl1.DailyAmountReporting) AS DailyR
, SUM(pnl1.itdAmountReporting)   AS ITDR
, SUM(pnl1.MTDAmountReporting)   AS MTDR
, SUM(pnl1.YTDAmountReporting)   AS YTDR
, SUM(pnl1.EomItdAmountSource)    AS EOMITDS
, SUM(pnl1.EomItdAmountPnl)       AS EOMITDP
, SUM(pnl1.EomItdAmountReporting) AS EOMITDR
, SUM(pnl1.EomItdAmountFreeze)    AS EOMITDF
, SUM(pnl1.EomItdAmountHO)        AS EOMITDH
, SUM(pnl1.EomYtdAmountSource)    AS EOMYTDS
, SUM(pnl1.EomYtdAmountPnl)       AS EOMYTDP
, SUM(pnl1.EomYtdAmountReporting) AS EOMYTDR
, SUM(pnl1.EomYtdAmountFreeze)    AS EOMYTDF
, SUM(pnl1.EomYtdAmountHO)        AS EOMYTDH
, SUM(pnl1.DailyAmountHO) AS DailyH
, SUM(pnl1.itdAmountHO)   AS ITDH
, SUM(pnl1.MTDAmountHO)   AS MTDH
, SUM(pnl1.YTDAmountHO)   AS YTDH
, SUM(pnl1.DailyAmountParadigm) AS DailyParadigm
, SUM(IIF(@SameMonth   = 1, pnl1.MtdAmountParadigm, 0)) AS MTDParadigm
, SUM(IIF(@SameQuarter = 1, pnl1.QtdAmountParadigm, 0)) AS QTDParadigm
, SUM(IIF(@SameYear    = 1, pnl1.YtdAmountParadigm, 0)) AS YTDParadigm
, PortfolioFlowIdPriorBalance AS FlowIdJoin
, MIN(CAST(WorkflowPnLDailyOpen AS SMALLINT)) AS WorkflowPnLDailyOpen
, CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END AS skcurrency
, 0 AS iscarryfreeze, isfx, pnl1.skportfolio
FROM #SellPrev pnl1 WITH (NOLOCK)
INNER JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
GROUP BY pnl1.skPnlTime, PortfolioID, pnl1.SkMeasureType, [SubMeasureTypeId], SkSourceCurrency, SkSourceData, PortfolioFlowIdPriorBalance,
         CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END, isfx, pnl1.skportfolio;

SELECT T.*, R.ratevalue, R.HOratevalue
INTO   #tmpBFELASTDay
FROM   #tmpBFELASTDaytmp T
INNER  JOIN #rate R ON  T.skportfolio = R.skportfolio
                    AND T.skpnltime   = R.datevalue
                    AND T.skcurrency  = R.skcurrency
                    AND T.isfx        = R.isfx
                    AND T.iscarryfreeze = R.isfreeze;

'

-- ===========================================================================
-- #RESBFE  --  variance check between current-day and prior-day aggregates.
-- [HIGH] image 4 + image 5 (sharp). FULL OUTER JOIN of the two tmpBFE
-- tables; per-measure CASE statements emit ''OK'' / ''MOK'' / ''KO''.
-- ===========================================================================
SET @Query = @Query + '

SELECT EOMITDP, EOMYTDP,
    d.Typ, dd.Typ AS Typ2,
    COALESCE(d.SkSourceData,     dd.SkSourceData)     AS SkSourceData,
    COALESCE(d.PortfolioID,      dd.PortfolioID)      AS PortfolioID,
    COALESCE(d.skmeasuretype,    dd.skmeasuretype)    AS skmeasuretype,
    COALESCE(d.skmeasuresubtype, dd.skmeasuresubtype) AS skmeasuresubtype,
    COALESCE(d.SkSourceCurrency, dd.SkSourceCurrency) AS SkSourceCurrency,
    d.isfx,

    COALESCE(d.DailyS, 0) AS DailySource,
    COALESCE(dd.ITDS,  0) AS PreviousITDSource,
    COALESCE(d.ITDS,   0) AS ITDSourceInDatabase,
    CASE
        WHEN ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) <= 1
             AND ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) != 0 THEN ''MOK''
        WHEN ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0))  = 0 THEN ''OK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown'' AND ABS(COALESCE(d.ITDS, 0) - COALESCE(d.MTDS, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailySourceVsITDSource,
    COALESCE(dd.MTDS, 0) AS PreviousMTDSource,
    COALESCE(d.MTDS,  0) AS MTDSourceInDatabase,
    CASE
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 5, 2) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 5, 2)
             AND ABS(COALESCE(d.DailyS, 0) - COALESCE(d.MTDS, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN ABS(COALESCE(dd.MTDS, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.MTDS, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.MTDS, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.MTDS, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailySourceVsMTDSource,
    COALESCE(dd.YTDS, 0) AS PreviousYTDSource,
    COALESCE(d.YTDS,  0) AS YTDSourceInDatabase,
    CASE
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 1, 4) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 1, 4)
             AND ABS(COALESCE(d.DailyS, 0) - COALESCE(d.YTDS, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN ABS(COALESCE(dd.YTDS, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.YTDS, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.YTDS, 0) + COALESCE(d.MTDS, 0) - COALESCE(d.YTDS, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailySourceVsYTDSource,

    COALESCE(d.DailyP, 0) AS DailyPnL,
    COALESCE(dd.ITDP,  0) AS PreviousITDPnL,
    COALESCE(d.ITDP,   0) AS ITDPnLInDatabase,
    CASE
        WHEN ABS(COALESCE(dd.itdP, 0) + COALESCE(d.DailyP, 0) - COALESCE(d.itdP, 0)) <= 1
             AND ABS(COALESCE(dd.itdP, 0) + COALESCE(d.DailyP, 0) - COALESCE(d.itdP, 0)) != 0 THEN ''MOK''
        WHEN ABS(COALESCE(dd.itdP, 0) + COALESCE(d.DailyP, 0) - COALESCE(d.itdP, 0))  = 0 THEN ''OK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown'' AND ABS(COALESCE(d.ItdP, 0) - COALESCE(d.MtdP, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyPNLvsITDPNL,
    COALESCE(dd.MTDP, 0) AS PreviousMTDPnL,
    COALESCE(d.MTDP,  0) AS MTDPnLInDatabase,
    CASE
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 5, 2) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 5, 2)
             AND ABS(COALESCE(d.DailyP, 0) - COALESCE(d.MTDP, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN ABS(COALESCE(dd.MTDP, 0) + COALESCE(d.DailyP, 0) - COALESCE(d.MTDP, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.MTDP, 0) + COALESCE(d.DailyP, 0) - COALESCE(d.MTDP, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyPNLvsMTDPNL,
    COALESCE(dd.YTDP, 0) AS PreviousYTDPnL,
    COALESCE(d.YTDP,  0) AS YTDPnLInDatabase,
    CASE
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 1, 4) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 1, 4)
             AND ABS(COALESCE(d.DailyP, 0) - COALESCE(d.YTDP, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN ABS(COALESCE(dd.YTDP, 0) + COALESCE(d.DailyP, 0) - COALESCE(d.YTDP, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.YTDP, 0) + COALESCE(d.MTDP, 0) - COALESCE(d.YTDP, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyPnLvsYTDPNL,

    COALESCE(d.DailyF, 0) AS DailyFreeze,
    COALESCE(dd.ITDF,  0) AS PreviousITDFreeze,
    COALESCE(d.ITDF,   0) AS ITDFreezeInDatabase,
    CASE
        WHEN ABS(COALESCE(dd.itdF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.itdF, 0)) <= 1
             AND ABS(COALESCE(dd.itdF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.itdF, 0)) != 0 THEN ''MOK''
        WHEN ABS(COALESCE(dd.itdF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.itdF, 0))  = 0 THEN ''OK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown'' AND ABS(COALESCE(d.MtdF, 0) - COALESCE(d.MtdF, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyFreezeVsITDFreeze,
    COALESCE(dd.MTDF, 0) AS PreviousMTDFreeze,
    COALESCE(d.MTDF,  0) AS MTDFreezeInDatabase,
    CASE
        WHEN ABS(COALESCE(dd.MtdF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.MtdF, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN ABS(COALESCE(dd.MtdF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.MtdF, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.MtdF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.MtdF, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyFreezeVsMTDFreeze,
    COALESCE(dd.YTDF, 0) AS PreviousYTDFreeze,
    COALESCE(d.YTDF,  0) AS YTDFreezeInDatabase,
    CASE
        WHEN ABS(COALESCE(dd.YTDF, 0) + COALESCE(d.DailyF, 0) - COALESCE(d.YTDF, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.eomYtdF, 0) + COALESCE(d.YtdF, 0) - COALESCE(d.YtdF, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyFreezeVsYTDFreeze,

    COALESCE(d.DailyR, 0) AS DailyReporting,
    COALESCE(dd.ITDR,  0) AS PreviousITDReporting,
    COALESCE(d.ITDR,   0) AS ITDReportingInDatabase,
    CASE
        WHEN d.iscarryfreeze = 0 AND d.isfx = 0 AND ABS(COALESCE(dd.itdS, 0) * d.ratevalue - COALESCE(d.itdR, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 0 AND d.isfx = 1 AND ABS(COALESCE(dd.itdS, 0) * d.ratevalue - COALESCE(d.itdR, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 1                AND ABS(COALESCE(dd.itdF, 0) * d.ratevalue - COALESCE(d.itdR, 0)) > 0.0001 THEN ''KO''
        WHEN ABS(COALESCE(dd.itdR, 0) + COALESCE(d.DailyR, 0) - COALESCE(d.itdR, 0)) <= 1 THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(d.eomitdR, 0) + COALESCE(d.itdR, 0) - COALESCE(d.MtdR, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyReportingVsITDReporting,
    COALESCE(dd.MTDR, 0) AS PreviousMTDReporting,
    COALESCE(d.MTDR,  0) AS MTDReportingInDatabase,
    CASE
        WHEN d.iscarryfreeze = 0 AND d.isfx = 0 AND ABS(COALESCE(dd.MtdS, 0) * d.ratevalue - COALESCE(d.MtdR, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 0 AND d.isfx = 1 AND ABS(COALESCE(dd.MtdS, 0) * d.ratevalue - COALESCE(d.MtdR, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 1                AND ABS(COALESCE(dd.MtdF, 0) * d.ratevalue - COALESCE(d.MtdR, 0)) > 0.0001 THEN ''KO''
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 5, 2) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 5, 2)
             AND ABS(COALESCE(d.DailyR, 0) - COALESCE(d.MTDR, 0)) <= @PrecisionAllowed THEN ''MOK''
        WHEN @IsEOM = 1 AND d.Typ = ''SellDown''
             AND ABS(COALESCE(dd.eomYtdR, 0) + COALESCE(d.itdR, 0) - COALESCE(d.MtdR, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyReportingVsMTDReporting,
    COALESCE(dd.YTDR, 0) AS PreviousYTDReporting,
    COALESCE(d.YTDR,  0) AS YTDReportingInDatabase,
    CASE
        WHEN d.iscarryfreeze = 0 AND d.isfx = 0 AND ABS(COALESCE(dd.YTDS, 0) * d.ratevalue - COALESCE(d.YTDR, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 0 AND d.isfx = 1 AND ABS(COALESCE(dd.YTDS, 0) * d.ratevalue - COALESCE(d.YTDR, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 1                AND ABS(COALESCE(dd.YTDF, 0) * d.ratevalue - COALESCE(d.YTDR, 0)) > 0.0001 THEN ''KO''
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 1, 4) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 1, 4)
             AND ABS(COALESCE(d.DailyR, 0) - COALESCE(d.YTDR, 0)) <= @PrecisionAllowed THEN ''MOK''
        WHEN MONTH(@PnlDate) = 1 AND ABS(COALESCE(d.YTDR, 0) - COALESCE(d.itdR, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyReportingVsYTDReporting,

    COALESCE(d.DailyH, 0) AS DailyHO,
    COALESCE(dd.ITDH,  0) AS PreviousITDHO,
    COALESCE(d.ITDH,   0) AS ITDHOInDatabase,
    CASE
        WHEN d.iscarryfreeze = 0 AND d.isfx = 0 AND ABS(COALESCE(dd.itdS, 0) * d.HOratevalue - COALESCE(d.itdH, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 0 AND d.isfx = 1 AND ABS(COALESCE(dd.itdS, 0) * d.HOratevalue - COALESCE(d.itdH, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 1                AND ABS(COALESCE(dd.itdF, 0) * d.HOratevalue - COALESCE(d.itdH, 0)) > 0.0001 THEN ''KO''
        WHEN ABS(COALESCE(dd.itdH, 0) + COALESCE(d.DailyH, 0) - COALESCE(d.itdH, 0)) <= 1 THEN ''MOK''
        ELSE ''KO''
    END AS StatusDailyHOVsITDHO,
    COALESCE(dd.MTDH, 0) AS PreviousMTDHO,
    COALESCE(d.MTDH,  0) AS MTDHOInDatabase,
    CASE
        WHEN d.iscarryfreeze = 0 AND d.isfx = 0 AND ABS(COALESCE(dd.MtdS, 0) * d.HOratevalue - COALESCE(d.MtdH, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 0 AND d.isfx = 1 AND ABS(COALESCE(dd.MtdS, 0) * d.HOratevalue - COALESCE(d.MtdH, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 1                AND ABS(COALESCE(dd.MtdF, 0) * d.HOratevalue - COALESCE(d.MtdH, 0)) > 0.0001 THEN ''KO''
        WHEN SUBSTRING(CONVERT(varchar(20), @PnlDate, 112), 5, 2) != SUBSTRING(CONVERT(varchar(20), @PrevPnl, 112), 5, 2)
             AND ABS(COALESCE(d.DailyH, 0) - COALESCE(d.MTDH, 0)) <= @PrecisionAllowed THEN ''MOK''
        WHEN MONTH(@PnlDate) = 1 AND ABS(COALESCE(d.MtdH, 0) - COALESCE(d.itdH, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyHOVsMTDHO,
    COALESCE(dd.YTDH, 0) AS PreviousYTDHO,
    COALESCE(d.YTDH,  0) AS YTDHOInDatabase,
    CASE
        WHEN d.iscarryfreeze = 0 AND d.isfx = 0 AND ABS(COALESCE(dd.YTDS, 0) * d.HOratevalue - COALESCE(d.YTDH, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 0 AND d.isfx = 1 AND ABS(COALESCE(dd.YTDS, 0) * d.HOratevalue - COALESCE(d.YTDH, 0)) > 0.0001 THEN ''KO''
        WHEN d.iscarryfreeze = 1                AND ABS(COALESCE(dd.YTDF, 0) * d.HOratevalue - COALESCE(d.YTDH, 0)) > 0.0001 THEN ''KO''
        WHEN ABS(COALESCE(dd.YTDH, 0) + COALESCE(d.DailyH, 0) - COALESCE(d.YTDH, 0)) <= @PrecisionAllowed THEN ''OK''
        WHEN MONTH(@PnlDate) = 1 AND ABS(COALESCE(d.YTDH, 0) - COALESCE(d.itdH, 0)) <= @PrecisionAllowed THEN ''OK''
        ELSE ''KO''
    END AS StatusDailyHOVsYTDHO,

    CASE WHEN ABS(COALESCE(dd.QTDParadigm, 0) + COALESCE(d.QTDParadigm, 0)) <= @PrecisionAllowed THEN ''OK''
         WHEN d.WorkflowPnLDailyOpen = 0 THEN ''OK''
         ELSE ''KO''
    END AS StatusQtdParadigm,
    CASE WHEN ABS(COALESCE(d.YTDParadigm, 0) + COALESCE(dd.QTDParadigm, 0) - COALESCE(d.YTDParadigm, 0)) <= @PrecisionAllowed THEN ''OK''
         WHEN d.WorkflowPnLDailyOpen = 0 THEN ''OK''
         ELSE ''KO''
    END AS StatusYtdParadigm,
    CASE WHEN ABS(COALESCE(dd.MTDParadigm, 0) + COALESCE(d.QTDParadigm, 0) - COALESCE(d.MTDParadigm, 0)) <= @PrecisionAllowed THEN ''OK''
         WHEN d.WorkflowPnLDailyOpen = 0 THEN ''OK''
         ELSE ''KO''
    END AS StatusMtdParadigm,

    COALESCE(dd.QTDParadigm,   d.QTDParadigm)   AS PreviousQtdParadigm, COALESCE(d.QTDParadigm,   dd.QTDParadigm)   AS QtdParadigmInDatabase,
    COALESCE(dd.MTDParadigm,   d.MTDParadigm)   AS PreviousMtdParadigm, COALESCE(d.MTDParadigm,   dd.MTDParadigm)   AS MtdParadigmInDatabase,
    COALESCE(dd.YTDParadigm,   d.YTDParadigm)   AS PreviousYtdParadigm, COALESCE(d.YTDParadigm,   dd.YTDParadigm)   AS YtdParadigmInDatabase,
    COALESCE(dd.DailyParadigm, d.DailyParadigm) AS DailyParadigm,
    COALESCE(d.FlowIdJoin,     dd.flowidjoin)   AS flowidjoin
INTO #RESBFE
FROM #tmpBFELASTDay dd
FULL OUTER JOIN #tmpBFEDay d
    ON  d.PortfolioID       = dd.PortfolioID
    AND d.SkMeasureType     = dd.SkMeasureType
    AND d.SkMeasureSubType  = dd.SkMeasureSubType
    AND d.SkSourceCurrency  = dd.SkSourceCurrency
    AND d.SkSourceData      = dd.SkSourceData
    AND d.Typ               = dd.Typ
    AND d.FlowIdJoin        = dd.FlowIdJoin
    AND d.isfx              = dd.isfx;

'

-- ===========================================================================
-- #FinalResult  --  string-concat of every KO flag, joined to the dimension
-- tables for human-readable names. [HIGH] image 6.
-- ===========================================================================
SET @Query = @Query + '

SELECT
    SUBSTRING(
        CASE WHEN StatusDailySourceVsITDSource         = ''KO'' THEN '', KO Itd Source''     ELSE '''' END +
        CASE WHEN StatusDailySourceVsMTDSource         = ''KO'' THEN '', KO Mtd Source''     ELSE '''' END +
        CASE WHEN StatusDailySourceVsYTDSource         = ''KO'' THEN '', KO Ytd Source''     ELSE '''' END +
        CASE WHEN StatusDailyPNLvsITDPNL               = ''KO'' THEN '', KO Itd PNL''        ELSE '''' END +
        CASE WHEN StatusDailyPNLvsMTDPNL               = ''KO'' THEN '', KO Mtd PNL''        ELSE '''' END +
        CASE WHEN StatusDailyPnLvsYTDPNL               = ''KO'' THEN '', KO Ytd PNL''        ELSE '''' END +
        CASE WHEN StatusDailyFreezeVsMTDFreeze         = ''KO'' THEN '', KO Mtd Freeze''     ELSE '''' END +
        CASE WHEN StatusDailyFreezeVsYTDFreeze         = ''KO'' THEN '', KO Ytd Freeze''     ELSE '''' END +
        CASE WHEN StatusDailyReportingVsMTDReporting   = ''KO'' THEN '', KO Mtd Reporting''  ELSE '''' END +
        CASE WHEN StatusDailyReportingVsYTDReporting   = ''KO'' THEN '', KO Ytd Reporting''  ELSE '''' END +
        CASE WHEN StatusDailyHOVsMTDHO                 = ''KO'' THEN '', KO Mtd HO''         ELSE '''' END +
        CASE WHEN StatusDailyHOVsYTDHO                 = ''KO'' THEN '', KO Ytd HO''         ELSE '''' END +
        CASE WHEN StatusMtdParadigm                    = ''KO'' THEN '', KO Mtd Paradigm''   ELSE '''' END +
        CASE WHEN StatusQtdParadigm                    = ''KO'' THEN '', KO Qtd Paradigm''   ELSE '''' END +
        CASE WHEN StatusYtdParadigm                    = ''KO'' THEN '', KO Ytd Paradigm''   ELSE '''' END
    , 3, 1000) AS [Status],
    COALESCE(Typ, Typ2) AS Typ,
    sd.SourceDataName,
    CAST(PortfolioId AS VARCHAR(50)) AS PortfolioName,
    DMT.MeasureTypeName, DMST.MeasureSubTypeName,
    DMST.SubMeasureTypeName, ccy.CurrencyCode,
    DailySource,    PreviousITDSource,    ITDSourceInDatabase,    PreviousMTDSource,    MTDSourceInDatabase,    PreviousYTDSource,    YTDSourceInDatabase,
    DailyPNL,       PreviousITDPNL,       ITDPNLInDatabase,       PreviousMTDPNL,       MTDPNLInDatabase,       PreviousYTDPNL,       YTDPNLInDatabase,
    DailyFreeze,    PreviousITDFreeze,    ITDFreezeInDatabase,    PreviousMTDFreeze,    MTDFreezeInDatabase,    PreviousYTDFreeze,    YTDFreezeInDatabase,
    DailyReporting, PreviousITDReporting, ITDReportingInDatabase, PreviousMTDReporting, MTDReportingInDatabase, PreviousYTDReporting, YTDReportingInDatabase,
    DailyHO,        PreviousITDHO,        ITDHOInDatabase,        PreviousMTDHO,        MTDHOInDatabase,        PreviousYTDHO,        YTDHOInDatabase,
    PreviousQtdParadigm, QtdParadigmInDatabase, PreviousMtdParadigm, MtdParadigmInDatabase, PreviousYtdParadigm, YtdParadigmInDatabase, DailyParadigm,
    flowidjoin AS FlowIdPreviousBalance
INTO #FinalResult
FROM #RESBFE t
INNER JOIN dwh.DimSourceData    sd   WITH (NOLOCK) ON sd.skSourceData       = t.SkSourceData
INNER JOIN dwh.DimMeasureType   DMT  WITH (NOLOCK) ON DMT.skmeasuretype     = t.skmeasuretype
INNER JOIN dwh.DimMeasureSubType DMST WITH (NOLOCK) ON DMST.skmeasuresubtype = t.skmeasuresubtype
INNER JOIN dwh.DimCurrency      ccy  WITH (NOLOCK) ON ccy.SkCurrency        = t.SkSourceCurrency
WHERE  StatusDailySourceVsMTDSource       = ''KO'' OR StatusDailySourceVsITDSource     = ''KO''
    OR StatusDailyPNLvsMTDPNL             = ''KO'' OR StatusDailyPNLvsITDPNL           = ''KO''
    OR StatusDailyFreezeVsMTDFreeze       = ''KO'' OR StatusDailyFreezeVsYTDFreeze     = ''KO''
    OR StatusDailyReportingVsMTDReporting = ''KO'' OR StatusDailyReportingVsYTDReporting = ''KO''
    OR StatusDailyHOVsMTDHO               = ''KO'' OR StatusDailyHOVsYTDHO             = ''KO''
    OR StatusDailySourceVsYTDSource       = ''KO''
    OR StatusQtdParadigm                  = ''KO'' OR StatusYtdParadigm                = ''KO''
    OR StatusMtdParadigm                  = ''KO''
ORDER BY PortfolioID, MeasureTypeName, MeasureSubTypeName, CurrencyCode, Typ;

INSERT INTO #FinalResult WITH (TABLOCK) ([Status], [SourceDataName], MeasureTypeName, CurrencyCode)
SELECT ''KO: Calculation of SourceSystem '' + SourceSystemName + '' is not finished yet'' AS [Status],
       SourceSystemName AS SourceDataName,
       '''' AS MeasureTypeName,
       '''' AS CurrencyCode
FROM   #ResultCheckBatchStatus
WHERE  CalculationIsDone = 0;

SELECT * FROM #FinalResult;

'

IF @Execute = 1
    EXEC sp_executesql @Query,
         N'@PnlDate AS DATE, @SourceSystemCodes AS VARCHAR(8000), @PrecisionAllowed DECIMAL(28,10)',
         @PnlDate, @SourceSystemCodes, @PrecisionAllowed

END
GO
