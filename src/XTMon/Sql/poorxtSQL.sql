CREATE PROCEDURE [monitoring].[UspXtgMonitoringPricingDaily]
(
    @PnlDate DATE,
    @SourceSystemCodes AS VARCHAR(4000) = NULL,
    @Execute BIT = 1,
    @Query NVARCHAR(MAX) = '' OUTPUT,
    @BookNames AS NVARCHAR(MAX) = NULL,
    @Group AS SMALLINT = NULL,
    @checkonlyBatchProcess BIT = 0
)
AS
BEGIN

DECLARE @ScopeBatchProcess NVARCHAR(MAX) = ''

IF (@checkonlyBatchProcess = 1)
BEGIN
    SET @ScopeBatchProcess = '
    WHEN IntegrateStatus IN (@COMP_ERR, @COMP) AND [SynchroStatus] IN (@COMP_ERR, @COMP) AND [PostSyncStatus] IN (@PGRS, @NOT_START, @COMP_ERR, @COMP) THEN ''OK'''
END

SET @Query = '
DECLARE @NOT_START SMALLINT, @COMP SMALLINT, @FAIL SMALLINT, @COMP_ERR SMALLINT,
        @PGRS SMALLINT, @CANCEL SMALLINT, @DELAY SMALLINT, @DELAY_FU_X SMALLINT, 
        @DELAY_FU SMALLINT, @DELAY_FX SMALLINT, @bankPnlReportingSystem NVARCHAR(200), 
        @thereIsRows BIT = 0

SELECT @NOT_START = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''NOT_START''
SELECT @COMP = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''COMP''
SELECT @FAIL = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''FAIL''
SELECT @COMP_ERR = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''COMP_ERR''

SELECT @PGRS = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''PGRS''
SELECT @CANCEL = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''CANCEL''
SELECT @DELAY = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''DELAY''
SELECT @DELAY_FU_X = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''DELAY_FU_X''
SELECT @DELAY_FU = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''DELAY_FU''
SELECT @DELAY_FX = StatusId FROM [LOG_FI_ALMT].administration.Status WITH(NOLOCK) WHERE StatusCode = ''DELAY_FX''

SELECT @bankPnlReportingSystem = Rules FROM [LOG_FI_ALMT].administration.TechnicalCROSSParameters WHERE Type = ''bankPnlReportingSystem''

DECLARE @LastEOM AS DATE
SELECT @LastEOM = administration.fn_GetPreviousBusinessDate(NULL, DATEADD(d, 1, EOMONTH(DATEADD(m, -1, @PnlDate))))

DECLARE @FeedSourceAdj AS SMALLINT
SELECT @FeedSourceAdj = FeedSourceID FROM [LOG_FI_ALMT].administration.FeedSources WITH(NOLOCK) WHERE FeedSourceCode = ''XTARG_ADJ''

DECLARE @BusinessDataTypeIdCA AS SMALLINT, @BusinessDataTypeIdPV AS SMALLINT, 
        @BusinessDataTypeIdCAP AS SMALLINT, @BusinessDataTypeIdPVP AS SMALLINT
SELECT @BusinessDataTypeIdCA = [BusinessDataTypeId] FROM [LOG_FI_ALMT].[administration].[BusinessDataTypes] WITH(NOLOCK) WHERE [BusinessDataTypeCode] = ''CAINIT''
SELECT @BusinessDataTypeIdPV = [BusinessDataTypeId] FROM [LOG_FI_ALMT].[administration].[BusinessDataTypes] WITH(NOLOCK) WHERE [BusinessDataTypeCode] = ''PVINIT''
SELECT @BusinessDataTypeIdCAP = [BusinessDataTypeId] FROM [LOG_FI_ALMT].[administration].[BusinessDataTypes] WITH(NOLOCK) WHERE [BusinessDataTypeCode] = ''CA''
SELECT @BusinessDataTypeIdPVP = [BusinessDataTypeId] FROM [LOG_FI_ALMT].[administration].[BusinessDataTypes] WITH(NOLOCK) WHERE [BusinessDataTypeCode] = ''PV''
'

SET @Query = @Query + '
CREATE TABLE #FS (FeedSourceId INT, SkSourceSystem INT)
INSERT INTO #FS (FeedSourceId, SkSourceSystem)
SELECT fs.FeedSourceId, ss.SkSourceSystem
FROM dwh.DimSourceSystem ss WITH(NOLOCK)
INNER JOIN dwh.DimSourceData sd WITH(NOLOCK) ON ss.SkSourceSystem = sd.SkSourceSystem
INNER JOIN [LOG_FI_ALMT].administration.FeedSources fs WITH(NOLOCK) ON sd.SourceDataCode = fs.FeedSourceCode
WHERE ss.SourceSystemCode IN ('' + COALESCE(@SourceSystemCodes, '''''') + '')
AND fs.FeedSourceId <> @FeedSourceAdj

SET @thereIsRows = @@ROWCOUNT
IF @thereIsRows = 0
BEGIN
    INSERT INTO #FS (FeedSourceId, SkSourceSystem)
    SELECT fs.FeedSourceId, sd.SkSourceSystem
    FROM [LOG_FI_ALMT].administration.FeedSources fs WITH(NOLOCK)
    INNER JOIN dwh.DimSourceData sd WITH(NOLOCK) ON sd.SourceDataCode = fs.FeedSourceCode
    AND fs.FeedSourceId <> @FeedSourceAdj
END
'

SET @Query = @Query + '
CREATE TABLE #Books (SkNoBook INT, SkMappingGroup INT)
INSERT INTO #Books (SkNoBook, SkMappingGroup)
SELECT DISTINCT book.SkNoBook, book.SkMappingGroup
FROM dwh.DimNoBook book WITH(NOLOCK)
WHERE book.BookName IN ('' + COALESCE(@BookNames, '''''') + '')
AND @PnlDate BETWEEN SkValidityDateStart AND SkValidityDateEnd

SET @thereIsRows = @@ROWCOUNT
IF @thereIsRows = 0
BEGIN
    INSERT INTO #Books (SkNoBook, SkMappingGroup)
    SELECT DISTINCT book.SkNoBook, book.SkMappingGroup
    FROM dwh.DimNoBook book WITH(NOLOCK)
    WHERE @PnlDate BETWEEN SkValidityDateStart AND SkValidityDateEnd
END
'

SET @Query = @Query + '
CREATE TABLE #SignOffGroups (SkMappingGroup INT)
INSERT INTO #SignOffGroups
SELECT DISTINCT SkMappingGroup
FROM [administration].[GroupSignOff] gs WITH(NOLOCK)
INNER JOIN #FS fs ON fs.SkSourceSystem = gs.SkSourceSystem
WHERE group_ = COALESCE(' + CAST(ISNULL(@Group, 'NULL') AS NVARCHAR(10)) + ', group_)

SET @thereIsRows = @@ROWCOUNT
IF @thereIsRows = 0
BEGIN
    INSERT INTO #SignOffGroups
    SELECT DISTINCT SkMappingGroup
    FROM [administration].[GroupSignOff] gs WITH(NOLOCK)
    INNER JOIN #FS fs ON fs.SkSourceSystem = gs.SkSourceSystem
END

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO #SignOffGroups
    SELECT DISTINCT SkMappingGroup FROM #Books
END
'

SET @Query = @Query + '
SELECT DISTINCT PortfolioName, BusinessDataTypeId, FeedSourceId, feedversion, TypeOfCalculation, pnldate,
RejectedRowsNettoed, FeedRowCount, PortfolioLiquidPriceBalance, PostSyncStatus, integratestatus, currentstep, workflowpnldailyopen
, SynchroStatus, IsFailed, eventtypeid, flowidderivedfrom, flowid
INTO #Scope
FROM [LOG_FI_ALMT].administration.Flows WITH (NOLOCK)
WHERE pnldate in (select distinct pnldate from #SATE)
AND BusinessDataTypeId IN (@BusinessDataTypeIdCAP, @BusinessDataTypeIdPVP) AND FeedSourceDerivedId = @FeedSourceAdj
'

SET @Query = @Query + '
SELECT DISTINCT CASE 
    WHEN IsEndOfMonthFile = 0 AND EventTypeName NOT IN (''ROLL OVER'') AND CurrentStep = ''Completed'' AND [administration].[fn_DayIsEndOfMonth](@pnlDate)=0 AND TypeOfCalculation IN (''P'',''W'',''H'') AND IntegrateStatus IN (@COMP, @COMP_ERR) THEN ''OK''
    WHEN IsEndOfMonthFile = 0 AND EventTypeName NOT IN (''ROLL OVER'') AND CurrentStep = ''Completed'' AND [administration].[fn_DayIsEndOfMonth](@pnlDate)=-1 AND TypeOfCalculation = ''H'' AND IntegrateStatus IN (@COMP, @COMP_ERR) THEN ''OK''
    WHEN IsEndOfMonthFile = 0 AND EventTypeName NOT IN (''ROLL OVER'') AND CurrentStep = ''Completed'' AND ([administration].[fn_DayIsEndOfMonth](@pnlDate)=0 OR nbtypeofcalc=1) AND TypeOfCalculation = ''S'' AND IntegrateStatus IN (@COMP, @COMP_ERR) THEN ''OK''
    WHEN (FeedSourceOutOfScope=1 OR PortfolioStatus IN (''CLOSED'',''INACTIVE'',''DORMANT'')) AND EventTypeName = ''ROLL OVER'' AND CurrentStep = ''Completed'' AND IntegrateStatus IN (@DELAY, @DELAY_FU_X, @DELAY_FU, @DELAY_FX) AND RejectedRowsNettoed = FeedRowCount AND PortfolioLiquidPriceBalance IS NULL THEN ''OK''
    WHEN IsEndOfMonthFile = 1 AND EventTypeName IN (''ROLL OVER'', ''BACKDATED TRADE'', ''DAILY PROCESS'') AND [administration].[fn_GetPreviousBusinessDate_N](NULL, @PnlDate, COALESCE(IsEndOfMonthFileExpectedDateAdd,0))=@LastEOM AND CurrentStep = ''Completed'' THEN ''OK''
    WHEN IsEndOfMonthFile = 1 AND EventTypeName = ''DAILY PROCESS'' AND [administration].[fn_GetPreviousBusinessDate_N](NULL, @PnlDate, COALESCE(IsEndOfMonthFileExpectedDateAdd,0))=@LastEOM AND CurrentStep = ''Completed'' THEN ''OK''
    WHEN administration.fn_IsWeek(0, @PnlDate)=1 AND ContainFuturePV=1 AND CurrentStep = ''Completed'' AND IntegrateStatus IN (@NOT_START, @COMP, @FAIL, @COMP_ERR) THEN ''OK''
    WHEN ExpectedMessageFile = 0 AND administration.fn_IsWeek(0, @PnlDate)=1 AND EventTypeName = ''ROLL OVER'' AND CurrentStep = ''Completed'' AND IntegrateStatus IN (@COMP_ERR, @COMP) THEN ''OK''
    WHEN IsDailyReception=0 AND CurrentStep = ''Completed'' AND IntegrateStatus IN (@COMP_ERR, @COMP) THEN ''OK''
    WHEN feeddelay=1 AND CurrentStep = ''Completed'' AND IntegrateStatus IN (@COMP_ERR, @COMP) THEN ''OK''
    ' + @ScopeBatchProcess + '
    ELSE ''KO'' END STATUS
, portfolioName, FeedSourceName, RebookingSystem, BusinessDataTypeName
, CASE WHEN CurrentStep = ''Completed'' AND IntegrateStatus IN (@DELAY, @DELAY_FU_X, @DELAY_FU, @DELAY_FX) THEN ''DELAYED'' ELSE CurrentStep END CurrentStep
, EventTypeName, FlowIdDerivedFrom, FlowId, pnldate, workflowpnldailyopen, FeedSourceId, IsFailed, TypeOfCalculation, PostSyncStatus, SynchroStatus
INTO #ResultTmp
FROM (
'

SET @Query = @Query + '
    SELECT [IsEndOfMonthFile], feeddelay, [TriggerExtractFromRollIfNoFeedDelay], p.portfolioName, FeedSourceName, p.RebookingSystem, p.BusinessDataTypeName,
    CASE WHEN CurrentStep IS NULL THEN ''MISSING'' ELSE CurrentStep END AS CurrentStep,
    s.StatusName, IntegrateStatus, EventTypeName, FlowIdDerivedFrom, f.flowid, COALESCE(f.pnldate, p.PnlDate) PnlDate, FeedVersion
    ,CASE 
        WHEN COALESCE(FeedVersion, 1)=MAX(COALESCE(FeedVersion, 1)) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
            ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 2 WHEN f.TypeOfCalculation = ''S'' THEN 1 WHEN f.TypeOfCalculation = ''H'' THEN 1 END) THEN 1
        WHEN COALESCE(FeedVersion, 1)=MAX(COALESCE(FeedVersion, 1)) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
            ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 1 WHEN f.TypeOfCalculation = ''S'' THEN 2 WHEN f.TypeOfCalculation = ''H'' THEN 1 END) THEN 1
        WHEN MAX(CASE WHEN WorkflowPnlDailyOpen = 1 THEN FeedVersion END) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
            ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 2 WHEN f.TypeOfCalculation = ''S'' THEN 1 WHEN f.TypeOfCalculation = ''H'' THEN 1 END)
            != MAX(COALESCE(FeedVersion, 1)) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
            ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 2 WHEN f.TypeOfCalculation = ''S'' THEN 1 WHEN f.TypeOfCalculation = ''H'' THEN 1 END)
            AND COALESCE(FeedVersion, 1)=MAX(CASE WHEN WorkflowPnlDailyOpen = 1 THEN FeedVersion END) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
            ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 2 WHEN f.TypeOfCalculation = ''S'' THEN 1 WHEN f.TypeOfCalculation = ''H'' THEN 1 END) THEN 1
    '

SET @Query = @Query + '
    WHEN MAX(CASE WHEN WorkflowPnlDailyOpen = 1 THEN FeedVersion END) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
        ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 1 WHEN f.TypeOfCalculation = ''S'' THEN 2 WHEN f.TypeOfCalculation = ''H'' THEN 1 END)
        != MAX(COALESCE(FeedVersion, 1)) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
        ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 1 WHEN f.TypeOfCalculation = ''S'' THEN 2 WHEN f.TypeOfCalculation = ''H'' THEN 1 END)
        AND COALESCE(FeedVersion, 1)=MAX(CASE WHEN WorkflowPnlDailyOpen = 1 THEN FeedVersion END) OVER (PARTITION BY f.portfolioName, f.BusinessDataTypeId, f.FeedSourceId, f.PnlDate 
        ORDER BY CASE WHEN f.TypeOfCalculation = ''P'' THEN 1 WHEN f.TypeOfCalculation = ''S'' THEN 2 WHEN f.TypeOfCalculation = ''H'' THEN 1 END) THEN 1
    ELSE 0 END MaxFeedVersion
    ,p.EndOfMonthFileExpectedDateAdd, FeedSourceOutOfScope, ContainFuturePV, ExpectedMessageFile, IsDailyReception, PortfolioStatus, WorkflowPnlDailyOpen, f.FeedSourceId,
    CASE WHEN f.IsFailed > 0 THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0) END IsFailed,
    f.RejectedRowsNettoed, f.FeedRowCount, f.PortfolioLiquidPriceBalance, f.TypeOfCalculation, f.PostSyncStatus, f.SynchroStatus, p.nbtypeofcalc
    FROM #PortfolioSystemContribution p WITH (NOLOCK)
    INNER JOIN dwh.DimSourceData DSD WITH (NOLOCK) ON p.FeedSourceCode=DSD.SourceDataCode
    INNER JOIN #books book ON book.SkNoBook = p.SkNoBook
    INNER JOIN #FS fs ON p.FeedSourceId = fs.FeedSourceId
    LEFT JOIN #scope f WITH (NOLOCK) ON p.PortfolioName=f.PortfolioName 
    AND p.BusinessDataTypeId=f.BusinessDataTypeId AND p.FeedSourceId=f.FeedSourceId AND f.pnldate=p.pnldate
    AND ((f.TypeOfCalculation=p.TypeOfCalculation) OR f.TypeOfCalculation=''H'')

    LEFT JOIN [LOG_FI_ALMT].administration.Status S WITH (NOLOCK) ON CASE WHEN f.IntegrateStatus=@COMP_ERR THEN 1 ELSE f.IntegrateStatus END=S.StatusId
    LEFT JOIN [LOG_FI_ALMT].administration.EventTypes e WITH (NOLOCK) ON f.EventTypeId=e.EventTypeId
'

SET @Query = @Query + '
    WHERE p.BusinessDataTypeId NOT IN (@BusinessDataTypeIdCA, @BusinessDataTypeIdPV)
    -- On affiche en missing les EOM, juste le jour du end of month + le nombre de jour expected après. Les autres jours, le fichier n''est pas attendu
    AND p.feedsourceid<>@FeedSourceAdj
) T
WHERE (MaxFeedVersion=1 OR CurrentStep=''MISSING'')
ORDER BY CurrentStep desc
'

SET @Query = @Query + '
SELECT [STATUS], R.portfolioName, FeedSourceName, RebookingSystem, BusinessDataTypeName, CurrentStep, EventTypeName, R.FlowIdDerivedFrom, flowid, pnldate,
xmlvalue=(SELECT [STATUS], R.portfolioName, FeedSourceName, RebookingSystem, BusinessDataTypeName, CurrentStep, EventTypeName, R.FlowIdDerivedFrom, flowid, pnldate FOR XML PATH(''PricingDaily''), type, elements absent)
,IsFailed, TypeOfCalculation
FROM #resulttmp R
'

IF @Execute=1
    EXEC sp_executesql @Query, N'@PnlDate AS DATE, @Group AS SMALLINT', @PnlDate, @Group

   

END
GO

USE [STAGING_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [monitoring].[UspXtgMonitoringBalancesCalculation]
(
    @PnLDate DATE,
    @Execute BIT = 1,
    @Query NVARCHAR(MAX) OUTPUT,
    @SourceSystemCodes VARCHAR(8000) = NULL,
    @PrecisionAllowed DECIMAL(28,10) = 0.01
)
AS
BEGIN
    SET NOCOUNT ON;
    SET @SourceSystemCodes = REPLACE(@SourceSystemCodes, ' ', '');

    SET @Query = ''

/*
DROP TABLE #asfunding3s
DROP TABLE #asfundingspread
DROP TABLE #carryfreeze
DROP TABLE #DimFlowType
DROP TABLE #lookuppratepricing
DROP TABLE #lookupprateSell
DROP TABLE #lookupprate
DROP TABLE #lookuppratefreeze
DROP TABLE #scopemarketdata
DROP TABLE #rate
DROP TABLE #asfunding
DROP TABLE #AdjustmentToMerge
DROP TABLE #flowstep
DROP TABLE #flowsfinal
DROP TABLE #flowfinalCurrent
DROP TABLE #flowfinalCurrentAdj
DROP TABLE #flowfinalPrev
DROP TABLE #flowfinalPrevAdj
DROP TABLE #pnl
DROP TABLE #sell
DROP TABLE #pnlprev
DROP TABLE #sellprev
DROP TABLE #tmpBFEDaytmp
DROP TABLE #tmpBFEDay
DROP TABLE #tmpBFELastDaytmp
DROP TABLE #tmpBFELastDay
DROP TABLE #RESBFE
DROP TABLE #FS
DROP TABLE #SS
DROP TABLE #ResultCheckBatchStatus
DROP TABLE #ResultCheckBatchStatus_FS
DROP TABLE #FinalResult
*/

DECLARE @PrevPnL AS DATE, @PrevPnLMonth AS DATE
DECLARE @FeedSourceIdAdj AS SMALLINT
DECLARE @IsEOM BIT, @IsAdjSplitted BIT
DECLARE @PVINIT SMALLINT, @CAINIT SMALLINT, @CA SMALLINT, @PV SMALLINT, @skmeasuretypeCSH SMALLINT, @skmeasuretypePV SMALLINT, @skmeasuretypeCRY SMALLINT, @skcurrencyEur SMALLINT
DECLARE @SameQuarter BIT, @SameMonth BIT, @SameYear BIT

SELECT @FeedSourceIdAdj = FeedSourceId FROM [LOG_FI_ALMT].[administration].[FeedSources] WITH (NOLOCK) WHERE [FeedSourceCode] = 'XTARG_ADJ'
SELECT @PrevPnL = [administration].[fn_GetPreviousBusinessDate](NULL, @PnLDate)
SELECT @PrevPnLMonth = [administration].[fn_GetLastDayOfMonth](NULL, DATEADD(d, 1, EOMONTH(@PnLDate, -1)))
SELECT @IsEOM = CASE WHEN [administration].[fn_GetLastDayOfMonth](NULL, @PnLDate) = @PnLDate THEN 1 ELSE 0 END
SET @SameQuarter = IIF(DATEPART(QUARTER, @PnLDate) = DATEPART(QUARTER, @PrevPnL), 1, 0)
SET @SameMonth = IIF(DATEPART(MONTH, @PnLDate) = DATEPART(MONTH, @PrevPnL), 1, 0)
SET @SameYear = IIF(DATEPART(YEAR, @PnLDate) = DATEPART(YEAR, @PrevPnL), 1, 0)

SELECT @CAINIT = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH(NOLOCK) WHERE BusinessDataTypeCode = 'CAINIT'
SELECT @PVINIT = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH(NOLOCK) WHERE BusinessDataTypeCode = 'PVINIT'

SELECT @CA = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH(NOLOCK) WHERE BusinessDataTypeCode = 'CA'
SELECT @PV = BusinessDataTypeID FROM [LOG_FI_ALMT].administration.BusinessDataTypes WITH(NOLOCK) WHERE BusinessDataTypeCode = 'PV'
SELECT @skcurrencyEur = SkCurrency FROM dwh.DimCurrency WHERE CurrencyCode = 'EUR'

SELECT @skmeasuretypeCSH = skmeasuretype FROM dwh.DimMeasureType WITH (nolock) WHERE MeasureTypeName = 'CASH'
SELECT @skmeasuretypePV = skmeasuretype FROM dwh.DimMeasureType WITH (nolock) WHERE MeasureTypeName = 'PV'
SELECT @skmeasuretypeCRY = skmeasuretype FROM dwh.DimMeasureType WITH (nolock) WHERE MeasureTypeName = 'CARRY'

SELECT SkFundingType, 2 AS FundingTypeKind INTO #asfundingOn FROM dwh.DimFundingType WITH (nolock) WHERE FundingRateType = 'CARRY O/N'
SELECT SkFundingType, 3 AS FundingTypeKind INTO #asfundingspread FROM dwh.DimFundingType WITH (nolock) WHERE FundingRateType = 'CARRY SPREAD'

SELECT @IsAdjSplitted = IsAdjSplitted FROM [LOG_FI_ALMT].[administration].[Xtarget_Parameters] WITH(NOLOCK)

SELECT DISTINCT IOPSkMeasureSubType INTO #carryfreeze FROM dwh.DimMeasureSubType WITH (nolock) WHERE IOPSkMeasureSubType IS NOT NULL

SELECT F.*, CASE WHEN C.IOPSkMeasureSubType IS NOT NULL THEN 1 ELSE 0 END iscarryfreeze INTO #DimFlowType FROM dwh.DimFlowType F WITH (nolock) LEFT JOIN #carryfreeze C ON C.IOPSkMeasureSubType = F.submeasuretypeid

DECLARE @FeedSourceAdj AS SMALLINT
SELECT @FeedSourceAdj = FeedSourceId FROM [LOG_FI_ALMT].administration.FeedSources WITH (NOLOCK) WHERE FeedSourceCode = 'XTARG_ADJ'

CREATE TABLE #ResultCheckBatchStatus (
    PnLDate DATE,
    SourceSystemName VARCHAR(2000),
    SkSourceSystem INT,
    CalculationIsDone INT,
    ConsoIsDone INT
)

INSERT INTO #ResultCheckBatchStatus WITH (TABLOCK)
EXEC [administration].[UspCheckBatchStatus] @PnLDate, @SourceSystemCodes

SELECT R.*, sd.SkSourceData, fs.FeedSourceId, ss.SourceSystemCode, CASE WHEN FAT.FeedSourceAssetTypeName = 'MONO_PORTFOLIO' THEN 1 ELSE 0 END AS ismono, FS.behavelikeadj
INTO #ResultCheckBatchStatus_FS FROM #ResultCheckBatchStatus R
INNER JOIN dwh.DimSourceData sd WITH(NOLOCK) ON R.SkSourceSystem = sd.SkSourceSystem
INNER JOIN dwh.DimSourceSystem ss WITH(NOLOCK) ON R.SkSourceSystem = ss.SkSourceSystem
INNER JOIN [LOG_FI_ALMT].administration.FeedSources fs WITH(NOLOCK) ON sd.SourceDataCode = fs.FeedSourceCode
INNER JOIN [LOG_FI_ALMT].administration.FeedSourceAssetType FAT WITH(NOLOCK) ON FAT.FeedSourceAssetTypeId = fs.FeedSourceAssetTypeId

CREATE TABLE #FS (FeedSourceId INT, SkSourceSystem INT, SkSourceData INT, ismono BIT, behavelikeadj BIT)

INSERT INTO #FS WITH (TABLOCK) (FeedSourceId, SkSourceSystem, SkSourceData, ismono, behavelikeadj)
SELECT R.FeedSourceId, R.SkSourceSystem, R.SkSourceData, ismono, R.behavelikeadj
FROM #ResultCheckBatchStatus_FS R
WHERE 1=1
AND R.FeedSourceId <> @FeedSourceAdj
AND R.CalculationIsDone = 1

SELECT DISTINCT SkSourceSystem INTO #SS FROM #FS

SET @Query = @Query + '

SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx 
INTO #lookuppratepricing
FROM dwh.FactPnLPricingAgg F WITH (nolock)
INNER JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE F.skpnltime = @PnLDate

INSERT INTO #lookuppratepricing WITH (TABLOCKX)
SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx 
FROM dwh.FactPnLPricingAggAdj F WITH (nolock)
INNER JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE F.skpnltime = @PnLDate
EXCEPT
SELECT sksourcecurrency, SkPortfolio, isfx
FROM #lookuppratepricing

SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx 
INTO #lookupprateSell
FROM dwh.FactPnLSellDownAgg F WITH (nolock)
INNER JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE F.skpnltime = @PnLDate

INSERT INTO #lookupprateSell WITH (TABLOCKX)
SELECT DISTINCT F.sksourcecurrency, F.SkPortfolio, F.isfx 
FROM dwh.FactPnLSellDownAggAdj F WITH (nolock)
INNER JOIN #FS fs ON fs.SkSourceData = F.SkSourceData
WHERE F.skpnltime = @PnLDate
EXCEPT
SELECT sksourcecurrency, SkPortfolio, isfx
FROM #lookupprateSell

SELECT * INTO #lookupprate FROM (
SELECT * FROM #lookuppratepricing
UNION
SELECT * FROM #lookupprateSell ) AS r

SELECT DISTINCT SkPortfolio, isfx INTO #lookuppratefreeze FROM #lookupprateSell

SELECT L.skportfolio, CASE WHEN isfx=0 THEN sksourcecurrency ELSE P.PnLSkCurrency END as skcurrency, P.ReportingSkCurrency, P.ReportingSkForexSet, isfx, 0 as isfreeze, P1.SkPortfolio as SkPortfolioPrevday
INTO #scopemarketdata
FROM #lookupprate L INNER JOIN dwh.DimPortfolio P ON P.skportfolio = L.skportfolio
LEFT JOIN dwh.DimPortfolio P1 ON P1.PortfolioId = P.PortfolioId AND @PrevPnL BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd
UNION
SELECT L.skportfolio, P.FreezingSkCurrency skcurrency, P.ReportingSkCurrency, P.ReportingSkForexSet, isfx, 1 as isfreeze, P1.SkPortfolio as SkPortfolioPrevday
FROM #lookuppratefreeze L INNER JOIN dwh.DimPortfolio P ON P.skportfolio = L.skportfolio
LEFT JOIN dwh.DimPortfolio P1 ON P1.PortfolioId = P.PortfolioId AND @PrevPnL BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd

SELECT DISTINCT E.ratevalue, CASE WHEN E.datevalue = @PrevPnL THEN SkPortfolioPrevday ELSE skportfolio END AS skportfolio, isfx, isfreeze, E.datevalue, skcurrency, EHO.ratevalue as Horatevalue
INTO #rate FROM #scopemarketdata S
INNER JOIN dwh.exchangerate E WITH (nolock)
ON S.skcurrency = E.FromSkCurrency AND S.ReportingSkCurrency = E.ToSkCurrency AND S.ReportingSkForexSet = E.SkForexSet AND E.IsLastVersion = 1 AND E.datevalue IN (@PnLDate, @PrevPnL)
INNER JOIN dwh.exchangerate EHO WITH (nolock)
ON S.skcurrency = EHO.FromSkCurrency AND EHO.ToSkCurrency = @skcurrencyEur AND S.ReportingSkForexSet = EHO.SkForexSet AND EHO.IsLastVersion = 1 AND EHO.datevalue IN (@PnLDate, @PrevPnL)
WHERE E.DateValue = EHO.DateValue

SELECT * INTO #skfunding FROM (
SELECT SkFundingType, FundingTypeKind FROM #asfundingspread
UNION ALL
SELECT SkFundingType, FundingTypeKind FROM #asfundingOn
UNION ALL
SELECT -1 AS SkFundingType, 1 AS FundingTypeKind ) AS r

SELECT U.PnLDate, U.Flowid, F.PackageGuid
INTO #AdjustmentToMerge
FROM [LOG_FI_ALMT].administration.Flows F WITH (nolock)
INNER JOIN (
    SELECT A.PnLDate, A.PortfolioFlowIdPriorBalance as flowid, F.PortfolioFlowIdPriorBalance
    FROM [LOG_FI_ALMT].administration.AdjustmentToMerge A WITH (nolock)
    INNER JOIN [LOG_FI_ALMT].administration.Flows F WITH (nolock) ON F.FlowId = A.Flowid AND F.pnldate = A.PnLDate
    INNER JOIN #FS fs ON fs.FeedSourceId = F.FeedSourceId
    WHERE A.PnLDate = @PnLDate
) AS u ON U.PortfolioFlowIdPriorBalance = F.PortfolioFlowIdPriorBalance AND U.PnLDate = F.PnLDate
WHERE FeedVersion = 1 AND F.pnldate = @PnLDate

SELECT [group], CASE WHEN feedsourceidderived <> @FeedSourceIdAdj THEN MAX(CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj THEN [Group] ELSE 0 END) OVER (partition by 1) ELSE 0 END AS [Group]
, FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance, PnLDate, BusinessDataTypeId, WorkflowPnLDailyOpen, PortfolioId, CoreProcessStatus
, CASE WHEN islastversion = 1 AND WorkflowPnLDailyOpen = 0 AND TypeOfCalculation IN (''P'', ''H'') AND coalesce(arkflowP, 0) < coalesce(arkflow1P, 0) THEN 0
    WHEN islastversion = 1 AND WorkflowPnLDailyOpen = 0 AND TypeOfCalculation IN (''S'', ''H'') AND coalesce(arkflowS, 0) < coalesce(arkflow1S, 0) THEN 0
       ELSE islastversion END AS islastversion, CASE WHEN feedsourcederivedid = @FeedSourceIdAdj THEN 1 WHEN behavelikeadj = 1 THEN 1 ELSE 0 END AS IsAdj
INTO #flowstep FROM (
    SELECT FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance, PnLDate, BusinessDataTypeId, islastversion, feedsourcederivedid
    , CASE WHEN packageguidORIG IS NULL THEN DENSE_RANK() OVER (order by PortfolioName, [BusinessDataTypeId], FeedSourceID)
           WHEN packageguidORIG IS NOT NULL THEN DENSE_RANK() OVER (order by packageguidORIG) END AS [Group], WorkflowPnLDailyOpen, PortfolioId, CoreProcessStatus
    , max(case when WorkflowPnLDailyOpen = 0 and TypeOfCalculation in (''P'',''H'') then FeedVersion END) over (partition by PortfolioFlowIdPriorBalance, pnldate) as arkflowP
    , max(case when WorkflowPnLDailyOpen = 0 and TypeOfCalculation in (''S'',''H'') then FeedVersion END) over (partition by PortfolioFlowIdPriorBalance, pnldate) as arkflowS
    , max(case when WorkflowPnLDailyOpen = 1 and TypeOfCalculation in (''P'',''H'') then FeedVersion END) over (partition by PortfolioFlowIdPriorBalance, pnldate) as arkflow1P
    , max(case when WorkflowPnLDailyOpen = 1 and TypeOfCalculation in (''S'',''H'') then FeedVersion END) over (partition by PortfolioFlowIdPriorBalance, pnldate) as arkflow1S
    , FeedVersion, TypeOfCalculation, behavelikeadj
    FROM (
        SELECT FlowIdDerivedFrom, F.Flowid, PortfolioFlowIdPriorBalance, F.PnLDate, BusinessDataTypeId, F.feedsourceid, PortfolioName, feedsourcederivedid, WorkflowPnLDailyOpen, Portfolioid
        , CASE WHEN feedversion = max(CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj THEN FeedVersion END) over (partition by PortfolioName, businessdatatypeid, F.pnldate, F.feedsourcederivedid, WorkflowPnLDailyOpen) THEN 1
               WHEN feedversion = max(CASE WHEN feedsourcederivedid = @FeedSourceIdAdj THEN FeedVersion END) over (partition by PortfolioName, businessdatatypeid, F.pnldate, F.feedsourcederivedid, WorkflowPnLDailyOpen) THEN 1
               ORDER BY CASE WHEN TypeOfCalculation = ''P'' THEN 2 WHEN TypeOfCalculation = ''S'' THEN 1 WHEN TypeOfCalculation = ''H'' THEN 3 END) AND feedsourcederivedid <> @FeedSourceIdAdj THEN 1
               WHEN feedversion = max(CASE WHEN feedsourcederivedid <> @FeedSourceIdAdj THEN FeedVersion END) over (partition by PortfolioFlowIdPriorBalance, pnldate, WorkflowPnLDailyOpen) AND feedsourcederivedid <> @FeedSourceIdAdj THEN 1
               ELSE 0 END AS islastversion, FeedVersion, TypeOfCalculation,
        IIF(MIN(CASE WHEN A.flowid IS NOT NULL THEN A.packageguid END) OVER (partition by F.PortfolioFlowIdPriorBalance, F.pnldate) IS NOT NULL,
            MIN(CASE WHEN A.flowid IS NOT NULL THEN A.packageguid END) OVER (partition by F.PortfolioFlowIdPriorBalance, F.pnldate),
            MIN(CASE WHEN feedversion = 1 AND FeedSourceIdAdj <> FeedSourceIdDerived THEN F.packageguid END) OVER (partition by F.PortfolioFlowIdPriorBalance, F.pnldate)) as packageguidORIG
        , CoreProcessStatus, behavelikeadj
        FROM [LOG_FI_ALMT].administration.Flows F WITH (nolock)
        INNER JOIN #FS fs ON fs.FeedSourceId = F.FeedSourceId
        LEFT JOIN #AdjustmentToMerge A ON A.FlowId = F.Flowid AND A.PnLDate = F.PnLDate
        WHERE (F.PnLDate = @PnLDate OR F.PnLDate = @PrevPnL) AND BusinessDataTypeId IN (@CA, @PV, @CAINIT, @PVINIT) AS r1
) AS r2
'

SET @Query = @Query + '

SELECT FlowIdDerivedFrom, FlowId, PortfolioFlowIdPriorBalance, PnLDate, BusinessDataTypeId, WorkflowPnLDailyOpen
, CASE WHEN MIN(CASE WHEN pnldate = @PrevPnL AND CoreProcessStatus = 1 THEN CAST(WorkflowPnLDailyOpen AS SMALLINT) END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) = 1 THEN 1
       WHEN COUNT(CASE WHEN pnldate = @PrevPnL AND CoreProcessStatus = 1 THEN 1 END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) > 1 THEN 1
       WHEN MIN(CASE WHEN pnldate = @PrevPnL AND CoreProcessStatus = 1 THEN CAST(WorkflowPnLDailyOpen AS SMALLINT) END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) = 0 THEN 0
       WHEN COUNT(CASE WHEN pnldate = @PrevPnL AND CoreProcessStatus = 1 THEN 1 END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) = 1 THEN 0
       ELSE 0 END AS ismirrored_prev
, CASE WHEN MIN(CASE WHEN pnldate = @PnLDate AND CoreProcessStatus = 1 THEN CAST(WorkflowPnLDailyOpen AS SMALLINT) END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) = 1 THEN 1
       WHEN COUNT(CASE WHEN pnldate = @PnLDate AND CoreProcessStatus = 1 THEN 1 END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) > 1 THEN 1
       WHEN MIN(CASE WHEN pnldate = @PnLDate AND CoreProcessStatus = 1 THEN CAST(WorkflowPnLDailyOpen AS SMALLINT) END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) = 0 THEN 0
       WHEN COUNT(CASE WHEN pnldate = @PnLDate AND CoreProcessStatus = 1 THEN 1 END) over (partition by [group], PortfolioFlowIdPriorBalance, pnldate) = 1 THEN 0
       ELSE 0 END AS ismirrored_curr
, ismirrored_adj
INTO #flowFinal FROM (
    SELECT [group], islastversion, IsAdj, FlowId
    , MAX(CASE WHEN WorkflowPnLDailyOpen = 1 AND pnldate = @PrevPnL THEN CAST(WorkflowPnLDailyOpen AS SMALLINT) END) over (partition by [group]) = 1 THEN 1
      MAX(CASE WHEN WorkflowPnLDailyOpen = 0 AND pnldate = @PrevPnL THEN F.Flowid END) over (partition by [group]) AS Flowid_prev
    , CASE WHEN IsTreasury = 1 THEN 1 ELSE 0 END AS ismirrored
    FROM dwh.DimPortfolio pl INNER JOIN [LOG_FI_ALMT].[administration].[Xtarget_Parameters] pl_p ON pl_p.IsTreasury = 1
    FROM #flowstep F
    INNER JOIN dwh.DimPortfolio pl ON F.PortfolioId = pl.PortfolioId AND F.PnLDate BETWEEN pl.SkValidityDateStart AND pl.SkValidityDateEnd
    WHERE islastversion = 1
) AS r

SELECT * INTO #flowsfinalCurrent FROM #flowFinal WHERE pnldate = @PnLDate AND (IsAdj = 0 OR @IsAdjSplitted = 0)
SELECT * INTO #flowsfinalCurrentAdj FROM #flowFinal WHERE pnldate = @PnLDate AND IsAdj = 1 AND @IsAdjSplitted = 1
SELECT * INTO #flowsfinalPrev FROM #flowFinal WHERE pnldate = @PrevPnL AND (IsAdj = 0 OR @IsAdjSplitted = 0)
SELECT * INTO #flowsfinalPrevAdj FROM #flowFinal WHERE pnldate = @PrevPnL AND IsAdj = 1 AND @IsAdjSplitted = 1

CREATE CLUSTERED INDEX IDX_FLOWSC ON #flowsfinalCurrent (Flowid)
CREATE CLUSTERED INDEX IDX_FLOWSCA ON #flowsfinalCurrentAdj (Flowid)
CREATE CLUSTERED INDEX IDX_FLOWSP ON #flowsfinalPrev (Flowid)
CREATE CLUSTERED INDEX IDX_FLOWSPA ON #flowsfinalPrevAdj (Flowid)

SELECT skpnltime, pnl1.SkPortfolio, SkMeasureType, SkMeasureSubType, SkSourceCurrency, pnl1.SkSourceData, pnl1.FlowId, isfx,
PortfolioFlowIdPriorBalance, WorkflowPnLDailyOpen, Isopenclosecurrentday, ismirrored, SkfundingType,
DailyAmountSource, CumulatedITDAmountSource, CumulatedMTDAmountSource, cryCumulatedDailyAmountSourceMirrored,
cryCumulatedDailyAmountSource, cryCumulatedDailyAmountSourceMirrored, MTDAmountSource, YTDAmountSource,
DailyAmountPnL, CumulatedITDAmountPnL, CumulatedMTDAmountPnL, cryCumulatedDailyAmountPnLMirrored,
DailyAmountPnL, CumulatedITDAmountPnL, cryCumulatedDailyAmountPnLMirrored,
cryCumulatedDailyAmountPnL, cryCumulatedDailyAmountPnLMirrored, MTDAmountPnL, YTDAmountPnL,
DailyAmountFreeze, CumulatedITDAmountFreeze, CumulatedMTDAmountFreeze, cryCumulatedDailyAmountFreezeMirrored,
DailyAmountFreeze, CumulatedITDAmountFreeze, cryCumulatedDailyAmountFreezeMirrored,
DailyAmountReporting, ITDAmountReporting, MTDAmountReporting, YTDAmountReporting,
DailyAmountHO, ITDAmountHO, MTDAmountHO, YTDAmountHO,
DailyAmountParadigm, MtdAmountParadigm, QtdAmountParadigm, YtdAmountParadigm
INTO #pnl
FROM dwh.FactPnLPricingAgg pnl1 WITH(NOLOCK)
INNER JOIN #flowsfinalCurrent f1 WITH(NOLOCK) ON pnl1.Flowid = f1.Flowid
INNER JOIN #FS fs ON fs.SkSourceData = pnl1.SkSourceData
WHERE pnl1.SkPnLTime = @PnLDate

INSERT INTO #pnl WITH (TABLOCKX)
SELECT skpnltime, pnl1.SkPortfolio, SkMeasureType, SkMeasureSubType, SkSourceCurrency, pnl1.SkSourceData, pnl1.FlowId, isfx,
PortfolioFlowIdPriorBalance, WorkflowPnLDailyOpen, Isopenclosecurrentday, ismirrored, SkfundingType,
DailyAmountSource, CumulatedITDAmountSource, CumulatedMTDAmountSource, cryCumulatedDailyAmountSourceMirrored,
cryCumulatedDailyAmountSource, cryCumulatedDailyAmountSourceMirrored, MTDAmountSource, YTDAmountSource,
DailyAmountPnL, CumulatedITDAmountPnL, CumulatedMTDAmountPnL, cryCumulatedDailyAmountPnLMirrored,
DailyAmountPnL, CumulatedITDAmountPnL, cryCumulatedDailyAmountPnLMirrored,
cryCumulatedDailyAmountPnL, cryCumulatedDailyAmountPnLMirrored, MTDAmountPnL, YTDAmountPnL,
DailyAmountFreeze, CumulatedITDAmountFreeze, CumulatedMTDAmountFreeze, cryCumulatedDailyAmountFreezeMirrored,
DailyAmountFreeze, CumulatedITDAmountFreeze, cryCumulatedDailyAmountFreezeMirrored,
DailyAmountReporting, ITDAmountReporting, MTDAmountReporting, YTDAmountReporting,
DailyAmountHO, ITDAmountHO, MTDAmountHO, YTDAmountHO,
DailyAmountParadigm, MtdAmountParadigm, QtdAmountParadigm, YtdAmountParadigm
FROM dwh.FactPnLPricingAggAdj pnl1 WITH(NOLOCK)
INNER JOIN #flowsfinalCurrentAdj f1 WITH(NOLOCK) ON pnl1.Flowid = f1.Flowid
INNER JOIN #FS fs ON fs.SkSourceData = pnl1.SkSourceData
WHERE pnl1.SkPnLTime = @PnLDate

'

-- Part 3: Variance Logic and sp_executesql
SET @Query = @Query + '

SELECT T.*, R.ratevalue, R.HOratevalue INTO #tmpBFEDay FROM #tmpBFEDaytmp T
INNER JOIN #rate R ON T.skportfolio = R.skportfolio
AND T.skpnltime = R.datevalue AND T.skcurrency = R.skcurrency
AND T.isfx = R.isfx AND T.iscarryfreeze = R.isfreeze

SELECT ''Pricing'' AS Typ, pnl1.skpnltime, PortfolioID, SkMeasureType, SkMeasureSubType, SkSourceCurrency, SkSourceData
,SUM(pnl1.DailyAmountSource) AS DailyS, SUM(CASE WHEN WorkflowPnLDailyOpen=0 THEN pnl1.CumulatedITDSource ELSE 0 END
    WHEN WorkflowPnLDailyOpen=1 and Isopencloseprevday=0 THEN pnl1.itdAmountSource ELSE 0 END
    +(CASE WHEN Isopencloseprevday=1 and SkMeasureType in (@skmeasuretypeCRY, @skmeasuretypePV) AND WorkflowPnLDailyOpen=1 then DailyAmountSource ELSE 0 END)
    -(CASE WHEN Isopencloseprevday=1 and SkMeasureType=@skmeasuretypePV AND WorkflowPnLDailyOpen=0 then CumulatedDailyAmountSource ELSE 0 END)
    WHEN Isopencloseprevday=1 and FundingTypeKind=2 AND WorkflowPnLDailyOpen=0 and ismirrored=1 and pl.IsTreasury=1 then cryCumulatedDailyAmountSourceMirrored
    WHEN Isopencloseprevday=1 and FundingTypeKind=2 AND WorkflowPnLDailyOpen=0 then cryCumulatedDailyAmountSource
    WHEN Isopencloseprevday=1 and FundingTypeKind=3 AND WorkflowPnLDailyOpen=0 then fcryCumulatedDailyAmountSource
    ELSE 0 END
    )) AS ITDS

-- ... (Similar SUM CASE blocks for MTDS, YTDS, DailyP, ITDP, MTDP, YTDP, DailyF, ITDF, MTDF, YTDF, DailyR, ITDR, MTDR, YTDR, DailyH, ITDH, MTDH, YTDH, Paradigm)

SELECT EOMITDP, EOMYTDP,
d.Typ, dd.Typ Typ2, COALESCE(d.SkSourceData, dd.SkSourceData) SkSourceData, COALESCE(d.PortfolioID, dd.PortfolioID) AS PortfolioID
, COALESCE(d.skmeasuretype, dd.skmeasuretype) skmeasuretype, COALESCE(d.skmeasuresubtype, dd.skmeasuresubtype) skmeasuresubtype
, COALESCE(d.SkSourceCurrency, dd.SkSourceCurrency) SkSourceCurrency, d.isfx

, COALESCE(d.DailyS, 0) DailySource, COALESCE(dd.itds, 0) AS PreviousITDSource, COALESCE(d.itds, 0) AS ITDSourceInDatabase
, CASE 
    WHEN ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) <= 1 AND ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) != 0 then ''MOK''
    WHEN ABS(COALESCE(dd.itds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.itds, 0)) == 0 then ''OK''
    WHEN @isEOM = 1 and d.Typ=''SellDown'' AND ABS(COALESCE(dd.itds, 0) + COALESCE(d.MTDS, 0) - COALESCE(d.itds, 0))<=@PrecisionAllowed THEN ''OK''
    ELSE ''KO'' END AS StatusDailySourceVsITDSource, COALESCE(dd.mtds, 0) AS PreviousMTDSource, COALESCE(d.mtds, 0) AS MTDSourceInDatabase
, CASE
    WHEN SUBSTRING(CONVERT(varchar(20), @PnLDate, 112), 5, 2) != SUBSTRING(CONVERT(varchar(20), @PrevPnL, 112), 5, 2) AND ABS(COALESCE(d.DailyS, 0) - COALESCE(d.mtds, 0))<=@PrecisionAllowed then ''OK''
    WHEN ABS(COALESCE(dd.mtds, 0) + COALESCE(d.DailyS, 0) - COALESCE(d.mtds, 0)) <= 1 then ''MOK''
    WHEN @isEOM = 1 and d.Typ=''SellDown'' AND ABS(COALESCE(d.mtds, 0) + COALESCE(d.mtds, 0) - COALESCE(d.mtds, 0))<=@PrecisionAllowed THEN ''OK''
    ELSE ''KO'' END AS StatusDailySourceVsMTDSource

-- ... (Additional Case Status logic for DailyP, MTDP, Freeze, Reporting, HO, Paradigm)

INTO #RESBFE
FROM #tmpBFELASTDay dd
FULL OUTER JOIN #tmpBFEDay d ON d.PortfolioID=dd.PortfolioID AND d.SkMeasureType=dd.SkMeasureType
AND d.SkMeasureSubType=dd.SkMeasureSubType AND d.SkSourceCurrency=dd.SkSourceCurrency AND d.SkSourceData=dd.SkSourceData AND d.Typ=dd.Typ AND d.FlowIdJoin=dd.FlowIdJoin
AND d.isfx=dd.isfx

SELECT 
SUBSTRING(CASE WHEN StatusDailySourceVsITDSource = ''KO'' THEN '', KO ITD Source'' ELSE '''' END +
          CASE WHEN StatusDailySourceVsMTDSource = ''KO'' THEN '', KO Mtd Source'' ELSE '''' END +
          CASE WHEN StatusDailyPNLvsITDPNL = ''KO'' THEN '', KO ITD PNL'' ELSE '''' END +
          CASE WHEN StatusDailyPNLvsMTDPNL = ''KO'' THEN '', KO Mtd PNL'' ELSE '''' END +
          CASE WHEN StatusDailyFreezeVsMTDFreeze = ''KO'' THEN '', KO Mtd Freeze'' ELSE '''' END +
          CASE WHEN StatusDailyReportingVsMTDReporting = ''KO'' THEN '', KO Mtd Reporting'' ELSE '''' END +
          CASE WHEN StatusDailyHOVsMTDHO = ''KO'' THEN '', KO Mtd HO'' ELSE '''' END +
          CASE WHEN StatusQtdParadigm = ''KO'' THEN '', KO Qtd Paradigm'' ELSE '''' END +
          CASE WHEN StatusYtdParadigm = ''KO'' THEN '', KO Ytd Paradigm'' ELSE '''' END
, 3, 1000) Status, Coalesce(Typ, Typ2) Typ
, SourceDataName, CAST(PortfolioId AS VARCHAR(50)) PortfolioName, MeasureTypeName, MeasureSubTypeName, SubMeasureTypeName, CurrencyCode
-- ... (Additional selected columns for result set)
into #FinalResult
FROM #RESBFE t
-- ... (Joins to DimSourceData, DimMeasureType, etc.)
WHERE (StatusDailySourceVsMTDSource = ''KO'' OR StatusDailySourceVsITDSource = ''KO'' OR StatusDailyPNLvsMTDPNL = ''KO'' ...)

INSERT INTO #FinalResult WITH (TABLOCK) ([Status], [SourceDataName], MeasureTypeName, CurrencyCode)
SELECT
''KO: Calculation of SourceSystem '' + SourceSystemName + '' is not finished yet'' as Status
, SourceSystemName as SourceDataName
, '''' as MeasureTypeName
, '''' as CurrencyCode
FROM #ResultCheckBatchStatus
WHERE CalculationIsDone = 0

select * from #FinalResult

'

IF @Execute=1
    EXEC sp_executesql @Query, N'@PnLDate DATE, @SourceSystemCodes VARCHAR(8000), @PrecisionAllowed DECIMAL(28,10)', @PnLDate, @SourceSystemCodes, @PrecisionAllowed;

END
GO