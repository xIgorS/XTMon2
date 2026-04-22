-- Backup of live Data Validation subitem procedures captured from SQL Server 10.211.55.2,1433 on 2026-04-22.

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [administration].[UspCheckBatchStatus]
@pnldate date=NULL, @sourcesytemcode VARCHAR(MAX)=NULL, @IsStandalone BIT=0, @Execute BIT=1, @Query NVARCHAR(max) = '' output -- @Execute is partially cut off
AS
begin
select pnldate,
    SourceSystemName,
    sksourcesystem,
    calculationisdone,
    ConsoIsDone,
    DatetimeEndCalculation,
    DatetimeEndExtraction
     from Replay.BatchStatus

select @Query = 'very long sql statement'
end
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringCheckReferentiel]
(@PnlDate AS DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT)

AS

BEGIN

SET @Query = 'VERY long long query'

SELECT TOP (1000) [Status]
      ,[FlowId]
      ,[EventTypeName]
      ,[BusinessDataTypeName]
      ,[FeedSourceName]
      ,[Location]
      ,[PnlDate]
      ,[ArrivalDate]
      ,[FileName]
      ,[CurrentStep]
FROM [Replay].[Referential]
--WHERE PnlDate = @PnlDate

END

GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringPricingReception]
(
    @PnlDate DATE, 
    @SourceSystemCodes AS VARCHAR(4000) = NULL, 
    @Execute BIT = 1, 
    @Query NVARCHAR(MAX) = '' OUTPUT, 
    @LastVersion BIT = 1
)
AS
BEGIN
    SET @Query = 'very longgggggg long sdfsd query'
    
    SELECT [Status]
          ,[XT]
          ,[FileName]
          ,[LoadStatus]
          ,[FeedSourceName]
          ,[FlowId]
          ,[BusinessDataTypeName]
          ,[PnlDate]
          ,[ArrivalDate]
          ,[FLAG_SLA]
          ,[SLATargetLocalTime]
          ,[CurrentStep]
          ,[feedsourceoutofscope]
    FROM [STAGING_FI_ALMT].[monitoring].[PFReception]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringOutOfScope]
    (@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
    SET @Query= 'SELECT F.portfolioname very long query'
    SELECT [portfolioname]
          ,[BusinessDataTypeCode]
          ,[CurrentStep]
          ,[EventTypeName]
          ,[flowid]
          ,[pnldate]
          ,[bookPnLReportingSystem]
          ,[islastversion]
          ,[FeedSourceCode]
    FROM [STAGING_FI_ALMT].[monitoring].[OSPortfolio]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspGetAdjNotlink]
@pnldate date=NULL, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT
AS
BEGIN
    SET @Query=' [monitoring].[UspGetAdjNotlink]'
    SELECT [flowidderivedfrom]
        ,[flowid]
        ,[portfolioflowidpriorbalance]
        ,[PortfolioID]
        ,[FeedsourceName]
        ,[BusinessDataTypeCode]
        ,[pnldate]
    FROM [STAGING_FI_ALMT].[monitoring].[AdjCheck]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringTradingVsFivr]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT)
AS
BEGIN
SET @Query = '[monitoring].[UspXtgMonitoringTradingVsFivr]'
SELECT [PortfolioId]
      ,[FIVR_Portfolioid]
      ,[bookPnLReportingSystem]
      ,[LegalEntityCode]
      ,[FIVR_LegalEntityCode]
      ,[LegalEntityLocation]
      ,[FIVR_LegalEntityLocation]
      ,[FreezingCurrency]
      ,[FIVR_Freezingcurrency]
      ,[TradingVsFivr_Check]
      ,[SignOff]
      ,[PnlGroup]
FROM [monitoring].[TradeFivr]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE monitoring.[UspXtgMonitoringCarrySpread]
(@PnlDate AS DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
begin
    SET @Query = 'one two three four five six'
    SELECT [PortFolioTrading]
          ,[PortFolioMirror]
          ,[FlowId]
          ,[CurrencyCode]
          ,[MeasureTypeName]
          ,[MeasureSubTypeName]
          ,[DailyAmountSource]
          ,[DailyAmountSourceTreasure]
          ,[DailyAmountFreeze]
          ,[DailyAmountFreezeTreasure]
    FROM [STAGING_FI_ALMT].[monitoring].[Mirror]
end
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE monitoring.[UspXtgMonitoringResultTransfer]
(@PnlDate AS DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
SET @Query='long long long long long long'
begin
    SELECT [PortfolioName]
          ,[CurrencyCode]
          ,[MeasureTypeName]
          ,[MeasureSubTypeName]
          ,[IsFx]
          ,[MtdAmount]
    FROM [STAGING_FI_ALMT].[monitoring].[resulttrasfer]
end
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringSAS]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT)
AS
begin
SET @Query = 'one two three four five .............'
SELECT [BusinessDataType]
      ,[Portfolio]
      ,[PnlDate]
      ,[ReportingDatePortfolio]
      ,[SourceData]
      ,[AdjustmentId]
      ,[FlowIdDerivedFrom]
      ,[FlowId]
FROM [STAGING_FI_ALMT].[monitoring].[Sas]
end
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [Monitoring].[UspXtgMonitoringPortfolioFlaggedXTG]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT) AS
BEGIN
SET @Query = 'wierdly long query for xt portfolios'
SELECT [Ptf_Name]
      ,[Ptf_Caption]
      ,[skPortFolio]
      ,[Init_Date]
      ,[Validity_start]
      ,[Validity_end]
      ,[Pnl_ReportingSystem]
      ,[Location]
      ,[Region]
      ,[Book_ID]
      ,[skMoBook]
      ,[SignOff]
      ,[PnlGroup]
FROM [STAGING_FI_ALMT].[monitoring].[NonXTPort]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [Monitoring].[UspXtgMonitoringPortfolioXTGRejected]
(@PnlDate DATE,@Execute BIT = 1,@Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
SET @Query='UspXtgMonitoringPortfolioXTGRejected    UspXtgMonitoringPortfolioXTGRejected'
SELECT [Portfolio_Name]
      ,[Reject_Description]
      ,[PnlGroup]
      ,[SignOff]
FROM [STAGING_FI_ALMT].[monitoring].[PtfReject]

END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringCheckFeedOutExtraction]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT)
AS
BEGIN
    SET @Query = 'very very very long long long'
    SELECT [Status], [PricingDailyStatus], [AdjustmentsStatus], [BcpStatus], [ExtractStatus], [BookName], [BookId], [ExtractionType]
    , [FirstDateExtract], [ScheduleTime], [ExtractionDate], [StartDate], [EndDate], [CorrelationId], [SourceSystemName]
    , [NbRows], [JobId], [ExtractName], [SkExtract], [PricingDailyCheckResult], [AdjustmentCheckResult], [FileSize]
    , [IsLast], [ExtractionFileName] FROM [STAGING_FI_ALMT].[monitoring].[FeedExtr]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringCheckFutureCash]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
SET @Query= 'very very very long long long'
SELECT [ValueDate]
    ,[PortfolioId]
    ,[FlowId]
    ,[FlowIdDerivedfrom]
    ,[FeedSource]
    ,[PnlDate]
    ,[SourceCurrency]
    ,[Amount]
    ,[NbRows]
FROM [STAGING_FI_ALMT].[monitoring].[FutureCash]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [Monitoring].[UspXtgMonitoringCheckConsistencyEvent]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
SET @Query='check check check             check'
SELECT [FlowId]
      ,[FlowIdDerivedFrom]
      ,[SkPortfolio]
      ,[PortfolioId]
      ,[FeedSourceCode]
      ,[Indicator]
      ,[DiffAmount]
      ,[FileName]
FROM [STAGING_FI_ALMT].[monitoring].[ConsistCheck]

END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringMultipleFeedVersion]
(@PnLDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
SET @Query=' [monitoring].[UspXtgMonitoringMultipleFeedVersion] '

SELECT [Portfolioid]
      ,[BusinessDataTypeId]
      ,[FeedSourceName]
      ,[PnLDate]
      ,[PortfolioFlowIdPriorBalance]
      ,[FeedVersion]
      ,[RecordCount]
 FROM [STAGING_FI_ALMT].[monitoring].[MultiFeed]

END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringBalancesCalculation]
(
     @PnlDate DATE
    ,@Execute BIT = 1
    ,@Query NVARCHAR(MAX) = '' OUTPUT
    ,@SourceSystemCodes VARCHAR(4000) = NULL
    ,@PrecisionAllowed DECIMAL(28,10) = 0.01
)
AS
BEGIN
    SET NOCOUNT ON;

    SET @Query = N'SELECT
         [Status]
        ,[Typ]
        ,[SourceDataName]
        ,[PortfolioName]
        ,[MeasureTypeName]
        ,[SubMeasureTypeName]
        ,[CurrencyCode]
        ,[DailySource]
        ,[PreviousITDSource]
        ,[ITDSourceInDatabase]
        ,[PreviousMTDSource]
        ,[MTDSourceInDatabase]
        ,[PreviousYTDSource]
        ,[YTDSourceInDatabase]
        ,[DailyPNL]
        ,[PreviousITDPNL]
        ,[ITDPNLInDatabase]
        ,[PreviousMTDPNL]
        ,[MTDPNLInDatabase]
        ,[PreviousYTDPNL]
        ,[YTDPNLInDatabase]
        ,[DailyFreeze]
        ,[PreviousITDFreeze]
        ,[ITDFreezeInDatabase]
        ,[PreviousMTDFreeze]
        ,[MTDFreezeInDatabase]
        ,[PreviousYTDFreeze]
        ,[YTDFreezeInDatabase]
        ,[DailyReporting]
        ,[PreviousITDReporting]
        ,[ITDReportingInDatabase]
        ,[PreviousMTDReporting]
        ,[MTDReportingInDatabase]
        ,[PreviousYTDReporting]
        ,[YTDReportingInDatabase]
        ,[DailyHO]
        ,[PreviousITDHO]
        ,[ITDHOInDatabase]
        ,[PreviousMTDHO]
        ,[MTDHOInDatabase]
        ,[PreviousYTDHO]
        ,[YTDHOInDatabase]
        ,[PreviousQtdParadigm]
        ,[QtdParadigmInDatabase]
        ,[PreviousMtdParadigm]
        ,[MtdParadigmInDatabase]
        ,[PreviousYtdParadigm]
        ,[YtdParadigmInDatabase]
        ,[DailyParadigm]
        ,[FlowIdPreviousBalance]
    FROM [monitoring].[DailyBalance];';

    IF ISNULL(@Execute, 1) = 1
    BEGIN
        EXEC sys.sp_executesql @Query;
    END
END;
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringAdjustments]
(
    @PnlDate DATE,
    @Execute BIT = 1,
    @SourceSystemCodes AS VARCHAR(4000) = NULL,
    @Query NVARCHAR(MAX) = '' OUTPUT,
    @BookNames AS NVARCHAR(MAX) = NULL,
    @Group AS SMALLINT = 0,
    @checkonlyBatchProcess BIT = 0
)
AS
BEGIN
 SET @Query = @SourceSystemCodes
    SELECT [Master_Flow]
        ,[Flow]
        ,[Prior_Balance]
        ,[Event_Type]
        ,[Business_Data_Type]
        ,[Feed_Source]
        ,[Pnl_Date]
        ,[Portfolio]
        ,[Step]
        ,[Last_Update]
        ,[IsFailed]
    FROM [STAGING_FI_ALMT].[monitoring].[Adjustments]
END

GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringPricingDaily]
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
    SET NOCOUNT ON;
    -- Build the dynamic SQL query
    SET @Query = @SourceSystemCodes
SELECT [STATUS]
      ,[EventTypeName]
      ,[CurrentStep]
      ,[flowId]
      ,[FlowIdDerivedFrom]
      ,[pnldate]
      ,[portfolioname]
      ,[FeedSourceName]
      ,[MoBookingSystem]
      ,[BusinessDataTypeName]
      ,[IsFailed]
      ,[TypeOfCalculation]
FROM [STAGING_FI_ALMT].[monitoring].[Pricing]

END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROC [monitoring].[UspXtgMTRevConWorkflowCheck]
(
    @PnlDate DATE,
    @SourceSystemCodes AS VARCHAR(4000) = NULL,
    @Execute BIT = 1,
    @Query NVARCHAR(MAX) = '' OUTPUT,
    @BookNames AS VARCHAR(4000) = NULL
)
As
BEGIN

    SET @Query = @SourceSystemCodes
    SELECT  [STATUS]
        ,[FeedSourceName]
        ,[BusinessDataTypeName]
        ,[CurrentStep]
        ,[FlowIdDerivedFrom]
        ,[flowId]
        ,[pnlDate]
        ,[IsFailed]
        ,[ArrivalDate]
        ,[xmlvalue]
    FROM [monitoring].[RevCon]
    where pnlDate = @PnlDate
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspGetPublicationConsistency]
    (@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT)
AS
BEGIN

    SET @Query = '[monitoring].[UspGetPublicationConsistency]'
    SELECT [Field]
          ,[flowid]
          ,[Message]
          ,[Arrivaldate]
          ,[SourceSystemCode]
    FROM [STAGING_FI_ALMT].[monitoring].[PubConsist]

END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [core_process].[UspCheckIfTableCreatedAfterPreCalc] 
    @pnlDate DATE, 
    @Execute BIT = 1, 
    @Query NVARCHAR(MAX) = '' OUTPUT
AS
BEGIN
    SET @Query = '[core_process].[UspCheckIfTableCreatedAfterPreCalc]'
    SELECT [PnlDate]
        , [ParentCorrelationId]
        , [TypeOfCalculation]
        , [PortfolioId]
        , [FeedSourceId]
        , [BusinessDataType]
        , [FlowId]
        , [KindOfProcessDescription]
        , [StatusProcess]
    FROM [core_process].[PreCalc]
END
GO

USE [STAGING_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspGetVRDBPricingReceptionStatus]
    @PnLDate DATE, 
    @Execute BIT = 1, 
    @Query NVARCHAR(MAX) = '' OUTPUT
AS
BEGIN
    SET @Query = '[monitoring].[UspGetVRDBPricingReceptionStatus]'

    SELECT [FileName]
          ,[GlobalBusinessLine]
          ,[AdjustmentType]
          ,[RecurrenceType]
          ,[Region]
          ,[IntegrationStatus]
          ,[Status]
          ,[PNLDate]
          ,[StartDate]
          ,[EndDate]
          ,[IsReload]
          ,[IsFailed]
    FROM [monitoring].[VRDBStat]
END
GO

USE [DTM_FI]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringMarketData]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX) = '' OUTPUT)
AS

BEGIN

SET @Query = 'very long long data'
SELECT TOP (1000) [SourceName]
    , [SourceType]
    , [NumberOfRateOnPnlDate]
    , [NumberOfRateOnMostRecentPnlDate]
    , [Result]
FROM [DTM_FI].[monitoring].[MarketData]
END
GO

USE [DTM_FI]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringMissingSOG]
(
    @PnlDate DATE,
    @Execute BIT = 1,
    @SourceSystemCodes AS VARCHAR(4000) = NULL,
    @Query NVARCHAR(MAX) = '' OUTPUT,
    @Isplatobehavior BIT = 1
)
AS
BEGIN

    SET @Query = '[monitoring].[UspXtgMonitoringMissingSOG]'

    SELECT [SkMappingGroup]
          ,[SkSourceSystem]
          ,[SourceSystemName]
    FROM [monitoring].[MissingSog]

END
GO

USE [DTM_FI]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringPTFRolled]
(@PnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
SET @Query = 'SELECT DISTINCT SkSourceData, SkSourceSystem, SkPortfolio'
    SELECT TOP (1000) [Source_System]
      ,[Source_Data]
      ,[PortfolioName]
  FROM [DTM_FI].[monitoring].[RolloverP]
END
GO

USE [LOG_FI_ALMT]
GO

CREATE OR ALTER PROCEDURE [Monitoring].[UspGetColStoreStatus]
(@PnlDate AS DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT)
AS
BEGIN
SET @Query = '[Monitoring].[UspGetColStoreStatus]'
SELECT [TableName]
    ,[DateCreated]
    ,[DateStarted]
    ,[DateUpdated]
    ,[StatusCode]
    ,[EventlogCorrelationId]
FROM [monitoring].[ColStore]
END
GO

USE [Publication]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspCheckJvBalanceBetweenTwoDates]
    @PnlDate DATE
    , @MetierCode VARCHAR(20) = 'FI'
    , @Execute BIT = 1
    , @Query NVARCHAR(MAX) = '' OUTPUT
    , @Precision DECIMAL(5, 2) = 0.01
AS
BEGIN
    SET @Query = '[monitoring].[UspCheckJvBalanceBetweenTwoDates] returns last column without name   ' + cast(@Precision as varchar(20))

    SELECT [PnlDate]
        , [portfolioid]
        , [MtdAmountHO]
        , [YtdAmountHO]
        , [MtdAmountParadigm]
        , [QtdAmountParadigm]
        , [YtdAmountParadigm]
        , [JvCheck]
        , [SourceSystemCode]
    FROM [monitoring].[JVBalConsist]
END
GO

USE [Publication]
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspCheckIfPortfolioCreatedInWorkflow] @pnlDate DATE, @Execute BIT = 1, @Query NVARCHAR(MAX)='' OUTPUT
AS
BEGIN

    SET @Query = 'UspCheckIfPortfolioCreatedInWorkflow'

    SELECT [PortfolioId]
          ,[SkPortfolio]
          ,[BookId]
          ,[BookName]
          ,[DailyValidatedAsset]
    FROM [monitoring].[MisWF]

END
GO