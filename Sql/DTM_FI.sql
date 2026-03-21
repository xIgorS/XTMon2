USE [DTM_FI]
GO
/****** Object:  StoredProcedure [Administration].[UspCleanUpJvissue]    Script Date: 2/27/2026 6:25:49 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Administration].[UspCleanUpJvissue] @PnlDate Date, @ExecuteCatchup bit = 0
	
AS
BEGIN	   
-- Wait for 25 seconds
        WAITFOR DELAY '00:00:15';

	SELECT CAST(@Pnldate as nvarchar(30)) + 'SELECT @PubliBehavior=[PubliBehavior]  from  [administration].[Publ_Parameters] with (NOLOCK);
SELECT @SkXtgSourceSystem= sksourcesystem from  publish.DlaSourceSystem WITH (NOLOCK) where SourceSystemCode= ''XTARGET'' ;
SELECT @SkPortfolioKindTrading=SkPortfolioKind  from [publish].[DimPortfolioKind]  where PortfolioKindName= ''Trading'' ;
SELECT @SkPortfolioKindAnalytic=SkPortfolioKind  from  [publish].[DimPortfolioKind] where PortfolioKindName= ''Analytic'' ;

CREATE TABLE #PortfolioJvCalculatedBySystem (JvReferentialSettingsId INT, SkPortfolioDest INT,PortfolioDestinationId INT,DailyAmountHO DECIMAL(28,10) ,MtdAmountHO  DECIMAL(28,10)
,YtdAmountHO DECIMAL(28,10), BackdatedKind SMALLINT,DailyAmountParadigm DECIMAL(28,10),QtdAmountParadigm DECIMAL(28,10),MtdAmountParadigm DECIMAL(28,10),YtdAmountParadigm DECIMAL(28,10)) ;

SELECT
PS.SkPortfolio as SkportfolioSource, PD.SkPortfolio as SkportfolioDest, R.JvId, R.PortfolioSourceId, R.PortfolioDestinationId, R.ReallocationRate, CAST(0 as decimal(26,10))   as PreviousReallocationRate,
R.JvReferentialSettingsId, 0 as IsBackdated, R.ValidationStartdate, R.ValidationEnddate, administration.fn_GetPreviousBusinessDate(R.ValidationStartdate, PS.MetierCode) as PreviousEndDate,
PS.DailyValidatedAsset, PS.MetierCode
INTO #PortfolioTrading
FROM  publish.JvReferentialSettings R WITH(NOLOCK)
INNER JOIN publish.DimPortfolio PS WITH(NOLOCK) ON R.PortfolioSourceId  = PS.PortfolioId AND @PnlLocalDate BETWEEN PS.SkValidityDateStart AND PS.SkValidityDateEnd
INNER JOIN publish.DimPortfolio PD WITH(NOLOCK) ON R.PortfolioDestinationId = PD.PortfolioId AND @PnlLocalDate BETWEEN PD.SkValidityDateStart AND PD.SkValidityDateEnd
WHERE PS.SkPortfolioKind = @SkPortfolioKindTrading or PS.SkPortfolioKind IS NULL AND @PnlLocalDate BETWEEN R.ValidationStartdate AND R.ValidationEnddate AND R.JvIsValid = 1 and PS.PortfolioIsParallelRun=0;

IF(@PubliBehavior=''V1'')
BEGIN

select distinct SkPortfolio INTO #listofScope from publish.factpnlpublish with (nolock) where skpnltime=@PnlLocalDate and sksourcesystem=@SkXtgSourceSystem;

DELETE FROM  #PortfolioTrading where DailyValidatedAsset=0;
DELETE FROM #PortfolioTrading where  SkportfolioSource in (select skportfolio from  #listofscope);

END

UPDATE P
SET P.IsBackdated = 1,
    P.PreviousReallocationRate = R.ReallocationRate
FROM #PortfolioTrading P INNER JOIN publish.JvReferentialSettings R WITH (NOLOCK)
ON P.JvId = R.JvId AND P.PortfolioSourceId = R.PortfolioSourceId AND P.PortfolioDestinationId = R.PortfolioDestinationId AND P.PreviousEndDate = R.ValidationEnddate
AND P.ReallocationRate <> R.ReallocationRate and P.ValidationStartdate=@PnlLocalDate

CREATE CLUSTERED INDEX IX_#PortfolioTrading_PortfolioSourceId ON #PortfolioTrading(SkportfolioSource);

select distinct  PortfolioDestinationId INTO #PortfolioTradingAgg from #PortfolioTrading;

SELECT distinct
PS.SkPortfolio as SkportfolioSource, PD.SkPortfolio as SkportfolioDest, R.JvId, R.PortfolioSourceId, R.PortfolioDestinationId, R.ReallocationRate, CAST(0 as decimal(26,10))   as PreviousReallocationRate,
R.JvReferentialSettingsId, 0 as IsBackdated, R.ValidationStartdate, R.ValidationEnddate, administration.fn_GetPreviousBusinessDate(R.ValidationStartdate, PS.MetierCode) as PreviousEndDate
INTO #PortfolioAnalytic
FROM publish.JvReferentialSettings R WITH(NOLOCK)
INNER JOIN publish.DimPortfolio PS WITH(NOLOCK) ON R.PortfolioSourceId = PS.PortfolioId AND @PnlLocalDate BETWEEN PS.SkValidityDateStart AND PS.SkValidityDateEnd
INNER JOIN publish.DimPortfolio PD WITH(NOLOCK) ON R.PortfolioDestinationId = PD.PortfolioId AND @PnlLocalDate BETWEEN PD.SkValidityDateStart AND PD.SkValidityDateEnd
INNER JOIN #PortfolioTradingAgg T ON T.PortfolioDestinationId= R.PortfolioSourceId
WHERE PS.SkPortfolioKind = @SkPortfolioKindAnalytic AND @PnlLocalDate BETWEEN R.ValidationStartdate AND R.ValidationEnddate AND R.JvIsValid = 1 and PS.PortfolioIsParallelRun=0;


UPDATE P
SET P.IsBackdated = 1,
    P.PreviousReallocationRate = R.ReallocationRate
FROM #PortfolioAnalytic P INNER JOIN publish.JvReferentialSettings R WITH (NOLOCK)
ON P.JvId = R.JvId AND P.PortfolioSourceId = R.PortfolioSourceId AND P.PortfolioDestinationId = R.PortfolioDestinationId AND P.PreviousEndDate = R.ValidationEnddate
AND P.ReallocationRate <> R.ReallocationRate and P.ValidationStartdate=@PnlLocalDate

select * INTO #PortfolioAnalyticNobackdated from  #PortfolioAnalytic  where IsBackdated=0;
select * INTO #PortfolioAnalyticBackdated from  #PortfolioAnalytic where IsBackdated=1;'
END
GO
