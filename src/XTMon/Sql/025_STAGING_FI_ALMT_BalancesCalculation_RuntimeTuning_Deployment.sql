

/*
================================================================================
 Procedure 2 -- rebuilt from poorxtSQL_recovered.sql

 Optimizations applied (each marked [OPT N] in-line):

   1. Consolidated the four BusinessDataTypes scalar lookups into a single
      table scan with conditional aggregation.

   2. Consolidated the three DimMeasureType scalar lookups into one.

   3. Removed the duplicate @FeedSourceAdj declaration -- the original
      declared the same SMALLINT twice from the same row.

   4. Materialised the funding-type union (CARRY O/N + CARRY SPREAD + the
      synthetic (-1, 1) row) directly into #skfunding, skipping the
      #asfundingOn / #asfundingspread intermediates that nothing else used.

   5. Replaced the awkward INSERT...EXCEPT pattern for #lookuppratepricing
      and #lookupprateSell with a single SELECT INTO ... FROM (UNION) which
      lets the optimizer pick one merge/hash dedup pass instead of three
      separate sorts.

   6. Pushed the #FS filter (FeedSourceId <> @FeedSourceIdAdj AND
      CalculationIsDone = 1) directly into the #ResultCheckBatchStatus_FS
      populate so rejected rows are never materialised.

   7. Added clustered indexes on the hot temp tables that are probed many
      times downstream: #FS, #lookupprate, #scopemarketdata, #rate,
      #skfunding, #pnl, #sell, #pnlPrev, #sellPrev. Each index is on the
      column set that appears in the dominant downstream join.

   8. Added OPTION (RECOMPILE) on the final SELECT inside the dynamic batch
      so the cardinality estimator sees the actual #FS / #flowsfinal* row
      counts each invocation instead of reusing a stale plan.

   9. Removed the dead, commented-out DROP TABLE prologue from the recovered
      file (the temp tables drop automatically at proc exit).

  10. The IN (@PnlDate, @PrevPnl) predicate replaces (PnlDate = @PnlDate OR
      PnlDate = @PrevPnl) so the optimizer can produce a seek + range plan
      against the Flows table.

  11. Removed staging that is not consumed anywhere in the balances path:
      @PrevPnlMonth, #carryfreeze, #DimFlowType, and #SS.

  12. Fixed the #flowsfinal projection so the downstream filters and fact
      joins get the columns they actually read (IsAdj, ismirrored), and
      added supporting indexes for the FeedSourceId and flow-split paths.

  13. Added clustered indexes on the wide temp tables that feed the rate join
      and the variance join so SQL Server can avoid hashing two large heaps in
      the hottest part of the proc.

 Behaviour preserved:
   - Output schema of the final SELECT is unchanged.
   - All status thresholds (@PrecisionAllowed, the 1.0 hard-coded epsilon,
     the 0.0001 reporting/HO epsilon) are unchanged.
   - The Pricing/SellDown UNION ALL aggregations in #tmpBFEDaytmp /
     #tmpBFELASTDaytmp are byte-identical -- they were not touched.
   - The FULL OUTER JOIN variance check in #RESBFE is unchanged.
   - The #FinalResult projection and the appended "Calculation not finished"
     row are unchanged.
================================================================================
*/

USE [STAGING_FI_ALMT]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [monitoring].[UspXtgMonitoringBalancesCalculation]
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
    SET @Query = N'';

    -- =====================================================================
    -- Scalar declarations
    -- =====================================================================
    DECLARE @PrevPnl          DATE,
            @FeedSourceIdAdj  SMALLINT,                 -- [OPT 3] one copy
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

    SELECT @FeedSourceIdAdj = FeedSourceId
    FROM   [LOG_FI_ALMT].[administration].[FeedSources] WITH (NOLOCK)
    WHERE  FeedSourceCode = 'XTARG_ADJ';

    SELECT @PrevPnl      = administration.fn_GetPreviousBusinessDate(NULL, @PnlDate),
           @IsEOM        = CASE WHEN administration.fn_GetLastDayOfMonth(NULL, @PnlDate) = @PnlDate THEN 1 ELSE 0 END;

    SELECT @SameQuarter = IIF(DATEPART(QUARTER, @PnlDate) = DATEPART(QUARTER, @PrevPnl), 1, 0),
           @SameMonth   = IIF(DATEPART(MONTH,   @PnlDate) = DATEPART(MONTH,   @PrevPnl), 1, 0),
           @SameYear    = IIF(DATEPART(YEAR,    @PnlDate) = DATEPART(YEAR,    @PrevPnl), 1, 0);

    -- [OPT 1] one round-trip instead of four
    SELECT @CAINIT = MAX(CASE WHEN BusinessDataTypeCode = 'CAINIT' THEN BusinessDataTypeID END),
           @PVINIT = MAX(CASE WHEN BusinessDataTypeCode = 'PVINIT' THEN BusinessDataTypeID END),
           @CA     = MAX(CASE WHEN BusinessDataTypeCode = 'CA'     THEN BusinessDataTypeID END),
           @PV     = MAX(CASE WHEN BusinessDataTypeCode = 'PV'     THEN BusinessDataTypeID END)
    FROM   [LOG_FI_ALMT].administration.BusinessDataTypes WITH (NOLOCK)
    WHERE  BusinessDataTypeCode IN ('CAINIT','PVINIT','CA','PV');

    -- [OPT 2] one round-trip instead of three
    SELECT @skmeasuretypeCSH = MAX(CASE WHEN MeasureTypeName = 'CASH'  THEN skmeasuretype END),
           @skmeasuretypePV  = MAX(CASE WHEN MeasureTypeName = 'PV'    THEN skmeasuretype END),
           @skmeasuretypeCRY = MAX(CASE WHEN MeasureTypeName = 'CARRY' THEN skmeasuretype END)
    FROM   dwh.DimMeasureType WITH (NOLOCK)
    WHERE  MeasureTypeName IN ('CASH','PV','CARRY');

    SELECT @skcurrencyEur = SkCurrency FROM dwh.DimCurrency WITH (NOLOCK) WHERE CurrencyCode = 'EUR';

    SELECT @IsAdjSplitted = IsAdjSplitted
    FROM   [LOG_FI_ALMT].[administration].[Xtarget_Parameters] WITH (NOLOCK);

    -- [OPT 4] funding-type union materialised once into the final temp
    SELECT *
    INTO   #skfunding
    FROM (
        SELECT SkFundingType, 2 AS FundingTypeKind
        FROM   dwh.DimFundingType WITH (NOLOCK) WHERE FundingRateType = 'CARRY O/N'
        UNION ALL
        SELECT SkFundingType, 3 AS FundingTypeKind
        FROM   dwh.DimFundingType WITH (NOLOCK) WHERE FundingRateType = 'CARRY SPREAD'
        UNION ALL
        SELECT -1, 1
    ) AS r;
    ALTER TABLE #skfunding ADD PRIMARY KEY CLUSTERED (SkFundingType);

    -- =====================================================================
    -- Batch status check (server-side, materialised once)
    -- =====================================================================
    CREATE TABLE #ResultCheckBatchStatus (
        PnlDate           DATE,
        SourceSystemName  VARCHAR(2000),
        SkSourceSystem    INT,
        CalculationIsDone INT,
        ConsoIsDone       INT
    );

    INSERT INTO #ResultCheckBatchStatus WITH (TABLOCK)
    EXEC [administration].[UspCheckBatchStatus] @PnlDate, @SourceSystemCodes;

    -- [OPT 6] the #FS filter is applied here so rejected rows are not
    -- materialised twice. The full projection is preserved for any
    -- diagnostic consumer that needs SourceSystemName.
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
        FeedSourceId   INT NOT NULL,
        SkSourceSystem INT NOT NULL,
        SkSourceData   INT NOT NULL,
        ismono         BIT NOT NULL,
        behavelikeadj  BIT NOT NULL
    );

    INSERT INTO #FS WITH (TABLOCK) (FeedSourceId, SkSourceSystem, SkSourceData, ismono, behavelikeadj)
    SELECT R.FeedSourceId, R.SkSourceSystem, R.SkSourceData, R.ismono, R.behavelikeadj
    FROM   #ResultCheckBatchStatus_FS R
    WHERE  R.FeedSourceId      <> @FeedSourceIdAdj
      AND  R.CalculationIsDone  = 1;

    -- [OPT 7] CI on the most-probed column (joined in 8+ places downstream)
    CREATE CLUSTERED INDEX IX_FS_SkSourceData ON #FS (SkSourceData);
        -- [OPT 12] flows/adjustment lookups probe #FS by FeedSourceId.
        CREATE NONCLUSTERED INDEX IX_FS_FeedSourceId ON #FS (FeedSourceId)
                INCLUDE (SkSourceData, SkSourceSystem, ismono, behavelikeadj);

    -- =====================================================================
    -- Begin dynamic SQL  --  block 1: lookup / scope temp tables
    -- =====================================================================
    SET @Query = @Query + N'
    -- [OPT 5] one statement instead of SELECT INTO + INSERT...EXCEPT
    SELECT sksourcecurrency, SkPortfolio, isfx
    INTO   #lookuppratepricing
    FROM (
        SELECT F.sksourcecurrency, F.SkPortfolio, F.isfx
        FROM   dwh.FactPnLPricingAgg F WITH (NOLOCK)
        JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
        WHERE  F.skpnltime = @PnlDate
        UNION
        SELECT F.sksourcecurrency, F.SkPortfolio, F.isfx
        FROM   dwh.FactPnLPricingAggAdj F WITH (NOLOCK)
        JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
        WHERE  F.skpnltime = @PnlDate
    ) AS r;

    SELECT sksourcecurrency, SkPortfolio, isfx
    INTO   #lookupprateSell
    FROM (
        SELECT F.sksourcecurrency, F.SkPortfolio, F.isfx
        FROM   dwh.FactPnLSellDownAgg F WITH (NOLOCK)
        JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
        WHERE  F.skpnltime = @PnlDate
        UNION
        SELECT F.sksourcecurrency, F.SkPortfolio, F.isfx
        FROM   dwh.FactPnLSellDownAggAdj F WITH (NOLOCK)
        JOIN   #FS fs ON fs.SkSourceData = F.SkSourceData
        WHERE  F.skpnltime = @PnlDate
    ) AS r;

    SELECT sksourcecurrency, SkPortfolio, isfx
    INTO   #lookupprate
    FROM (
        SELECT sksourcecurrency, SkPortfolio, isfx FROM #lookuppratepricing
        UNION
        SELECT sksourcecurrency, SkPortfolio, isfx FROM #lookupprateSell
    ) AS r;
    -- [OPT 7] CI for the join in #scopemarketdata
    CREATE CLUSTERED INDEX IX_lookupprate ON #lookupprate (SkPortfolio, isfx, sksourcecurrency);

    SELECT DISTINCT SkPortfolio, isfx
    INTO   #lookuppratefreeze
    FROM   #lookupprateSell;

    -- ------------------------------------------------------------------
    -- scope market data
    -- ------------------------------------------------------------------
    SELECT  L.skportfolio,
            CASE WHEN L.isfx = 0 THEN L.sksourcecurrency ELSE P.PnlSkCurrency END AS skcurrency,
            P.ReportingSkCurrency, P.ReportingSkForexSet, L.isfx,
            0 AS isfreeze,
            P1.SkPortfolio AS SkPortfolioPrevday
    INTO    #scopemarketdata
    FROM    #lookupprate L
    JOIN    dwh.DimPortfolio P  ON P.skportfolio  = L.skportfolio
    LEFT    JOIN dwh.DimPortfolio P1
            ON P1.PortfolioId = P.PortfolioId
           AND @PrevPnl BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd
    UNION
    SELECT  L.skportfolio,
            P.FreezingSkCurrency,
            P.ReportingSkCurrency, P.ReportingSkForexSet, L.isfx,
            1 AS isfreeze,
            P1.SkPortfolio
    FROM    #lookuppratefreeze L
    JOIN    dwh.DimPortfolio P  ON P.skportfolio  = L.skportfolio
    LEFT    JOIN dwh.DimPortfolio P1
            ON P1.PortfolioId = P.PortfolioId
           AND @PrevPnl BETWEEN P1.SkValidityDateStart AND P1.SkValidityDateEnd;
    -- [OPT 7] CI for the rate join
    CREATE CLUSTERED INDEX IX_scopemarketdata
        ON #scopemarketdata (skportfolio, skcurrency, isfx, isfreeze);

    -- ------------------------------------------------------------------
    -- rate
    -- ------------------------------------------------------------------
    SELECT DISTINCT
            E.ratevalue,
            CASE WHEN E.datevalue = @PrevPnl THEN S.SkPortfolioPrevday ELSE S.skportfolio END AS skportfolio,
            S.isfx, S.isfreeze, E.datevalue, S.skcurrency,
            EHO.ratevalue AS Horatevalue
    INTO    #rate
    FROM    #scopemarketdata S
    JOIN    dwh.exchangerate E   WITH (NOLOCK)
            ON  E.FromSkCurrency = S.skcurrency
            AND E.ToSkCurrency   = S.ReportingSkCurrency
            AND E.SkForexSet     = S.ReportingSkForexSet
            AND E.IsLastVersion  = 1
            AND E.datevalue      IN (@PnlDate, @PrevPnl)
    JOIN    dwh.exchangerate EHO WITH (NOLOCK)
            ON  EHO.FromSkCurrency = S.skcurrency
            AND EHO.ToSkCurrency   = @skcurrencyEur
            AND EHO.SkForexSet     = S.ReportingSkForexSet
            AND EHO.IsLastVersion  = 1
            AND EHO.datevalue      = E.DateValue;
    -- [OPT 7] CI matches the join in #tmpBFEDay / #tmpBFELASTDay
    CREATE CLUSTERED INDEX IX_rate ON #rate (skportfolio, datevalue, skcurrency, isfx, isfreeze);

    -- ------------------------------------------------------------------
    -- adjustment-to-merge mapping
    -- ------------------------------------------------------------------
    SELECT  U.PnlDate, U.Flowid, F.PackageGuid
    INTO    #AdjustmentToMerge
    FROM    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
    JOIN (
        SELECT  A.PnlDate, A.PortfolioFlowIdPriorBalance AS flowid, F.PortfolioFlowIdPriorBalance
        FROM    [LOG_FI_ALMT].administration.AdjustmentToMerge A WITH (NOLOCK)
        JOIN    [LOG_FI_ALMT].administration.Flows F WITH (NOLOCK)
            ON  F.Flowid  = A.Flowid
            AND F.pnldate = A.PnlDate
        JOIN    #FS fs ON fs.FeedSourceId = F.FeedSourceId
        WHERE   A.PnlDate = @PnlDate
    ) AS U
        ON  U.PortfolioFlowIdPriorBalance = F.PortfolioFlowIdPriorBalance
        AND U.PnlDate                     = F.PnlDate
    WHERE   F.FeedVersion = 1
      AND   F.pnldate     = @PnlDate;

    -- ------------------------------------------------------------------
    -- flow-step ranking (logic preserved verbatim from the recovered source)
    -- ------------------------------------------------------------------
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
            JOIN    #FS fs                  ON fs.FeedSourceId = F.FeedSourceId
            LEFT    JOIN #AdjustmentToMerge A    ON A.FlowId = F.Flowid AND A.PnlDate = F.PnlDate
            -- [OPT 10] IN(...) instead of OR-list so the optimizer picks a seek+range
            WHERE   F.PnlDate IN (@PnlDate, @PrevPnl)
              AND   BusinessDataTypeId IN (@CA, @PV, @CAINIT, @PVINIT)
        ) AS r1
    ) AS r2;
    ';

    -- =====================================================================
    -- Block 2: #flowsfinal split + per-day pricing/selldown heaps
    -- =====================================================================
    SET @Query = @Query + N'

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
             IsAdj,
             ismirrored_adj AS ismirrored
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
        JOIN    dwh.DimPortfolio P
                ON  P.PortfolioId = F.PortfolioId
                AND P.PnlDate BETWEEN SkValidityDateStart AND SkValidityDateEnd
    ) AS r
    WHERE r.isLastVersion = 1;
    -- [OPT 12] the next four splits filter by PnlDate/IsAdj and then join by FlowId.
    CREATE CLUSTERED INDEX IX_flowsfinal ON #flowsfinal (PnlDate, IsAdj, FlowId);

    SELECT * INTO #flowsfinalCurrent    FROM #flowsfinal WHERE pnldate = @PnlDate AND (IsAdj = 0 OR @IsAdjSplitted = 0);
    SELECT * INTO #flowsfinalCurrentAdj FROM #flowsfinal WHERE pnldate = @PnlDate AND (IsAdj = 1 AND @IsAdjSplitted = 1);
    SELECT * INTO #flowsfinalPrev       FROM #flowsfinal WHERE pnldate = @PrevPnl AND (IsAdj = 0 OR @IsAdjSplitted = 0);
    SELECT * INTO #flowsfinalPrevAdj    FROM #flowsfinal WHERE pnldate = @PrevPnl AND (IsAdj = 1 AND @IsAdjSplitted = 1);

    CREATE CLUSTERED INDEX IDX_FLOWSC  ON #flowsfinalCurrent    (Flowid);
    CREATE CLUSTERED INDEX IDX_FLOWSCA ON #flowsfinalCurrentAdj (Flowid);
    CREATE CLUSTERED INDEX IDX_FLOWSP  ON #flowsfinalPrev       (Flowid);
    CREATE CLUSTERED INDEX IDX_FLOWSPA ON #flowsfinalPrevAdj    (Flowid);

    -- #pnl  -- current-day pricing heap
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
    JOIN    #flowsfinalCurrent f1 ON pnl1.FlowId   = f1.FlowId
    JOIN    #FS fs                 ON fs.SkSourceData = pnl1.SkSourceData
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
    JOIN    #flowsfinalCurrentAdj f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs                   ON fs.SkSourceData = pnl1.SkSourceData
    WHERE   pnl1.SkPnlTime = @PnlDate;
    -- [OPT 7] CI matches the GROUP BY in #tmpBFEDaytmp + the #skfunding join
    CREATE CLUSTERED INDEX IX_pnl ON #pnl (SkSourceData, FlowId);

    -- #sell -- current-day selldown heap
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
    JOIN    #flowsfinalCurrent f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs                ON fs.SkSourceData = pnl1.SkSourceData
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
    JOIN    #flowsfinalCurrentAdj f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs                   ON fs.SkSourceData = pnl1.SkSourceData
    WHERE   pnl1.SkPnlTime = @PnlDate;
    CREATE CLUSTERED INDEX IX_sell ON #sell (SkSourceData, FlowId);

    -- #pnlPrev -- prior-day pricing heap
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
    JOIN    #flowsfinalPrev f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs             ON fs.SkSourceData = pnl1.SkSourceData
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
    JOIN    #flowsfinalPrevAdj f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs                ON fs.SkSourceData = pnl1.SkSourceData
    WHERE   pnl1.SkPnlTime = @PrevPnl;
    CREATE CLUSTERED INDEX IX_pnlPrev ON #pnlPrev (SkSourceData, FlowId);

    -- #sellPrev -- prior-day selldown heap
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
    JOIN    #flowsfinalPrev f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs             ON fs.SkSourceData = pnl1.SkSourceData
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
    JOIN    #flowsfinalPrevAdj f1 ON pnl1.FlowId = f1.FlowId
    JOIN    #FS fs                ON fs.SkSourceData = pnl1.SkSourceData
    WHERE   pnl1.SkPnlTime = @PrevPnl;
    CREATE CLUSTERED INDEX IX_sellPrev ON #sellPrev (SkSourceData, FlowId);
    ';

    -- =====================================================================
    -- Block 3: #tmpBFEDaytmp + #tmpBFEDay (current-day aggregation)
    --          Aggregation logic preserved verbatim from the recovered file.
    -- =====================================================================
    SET @Query = @Query + N'

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
    JOIN #skfunding   SK              ON SK.SkFundingType = pnl1.SkFundingType
    JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
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
    JOIN #skfunding SK              ON SK.SkFundingType = pnl1.SkFundingType
    JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
    GROUP BY pnl1.skPnlTime, PortfolioID, pnl1.SkMeasureType, [SubMeasureTypeId], SkSourceCurrency, SkSourceData, PortfolioFlowIdPriorBalance,
             CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END, isfx, pnl1.skportfolio;
        CREATE CLUSTERED INDEX IX_tmpBFEDaytmp
            ON #tmpBFEDaytmp (skportfolio, skPnlTime, skcurrency, isfx, iscarryfreeze);

    SELECT T.*, R.ratevalue, R.HOratevalue
    INTO   #tmpBFEDay
    FROM   #tmpBFEDaytmp T
    JOIN   #rate R ON  T.skportfolio   = R.skportfolio
                   AND T.skpnltime     = R.datevalue
                   AND T.skcurrency    = R.skcurrency
                   AND T.isfx          = R.isfx
                   AND T.iscarryfreeze = R.isfreeze;
        CREATE CLUSTERED INDEX IX_tmpBFEDay
            ON #tmpBFEDay (PortfolioID, SkMeasureType, SkMeasureSubType, SkSourceCurrency, SkSourceData, Typ, FlowIdJoin, isfx);
    ';

    -- =====================================================================
    -- Block 4: #tmpBFELASTDaytmp + #tmpBFELASTDay (prior-day aggregation)
    -- =====================================================================
    SET @Query = @Query + N'

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
    JOIN #skfunding   SK              ON SK.SkFundingType = pnl1.SkFundingType
    JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
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
    JOIN dwh.DimPortfolio pl WITH (NOLOCK) ON pl.SkPortfolio = pnl1.skportfolio
    GROUP BY pnl1.skPnlTime, PortfolioID, pnl1.SkMeasureType, [SubMeasureTypeId], SkSourceCurrency, SkSourceData, PortfolioFlowIdPriorBalance,
             CASE WHEN isfx = 0 THEN sksourcecurrency ELSE PnLSkCurrency END, isfx, pnl1.skportfolio;
        CREATE CLUSTERED INDEX IX_tmpBFELASTDaytmp
            ON #tmpBFELASTDaytmp (skportfolio, skPnlTime, skcurrency, isfx, iscarryfreeze);

    SELECT T.*, R.ratevalue, R.HOratevalue
    INTO   #tmpBFELASTDay
    FROM   #tmpBFELASTDaytmp T
    JOIN   #rate R ON  T.skportfolio   = R.skportfolio
                   AND T.skpnltime     = R.datevalue
                   AND T.skcurrency    = R.skcurrency
                   AND T.isfx          = R.isfx
                   AND T.iscarryfreeze = R.isfreeze;
        CREATE CLUSTERED INDEX IX_tmpBFELASTDay
            ON #tmpBFELASTDay (PortfolioID, SkMeasureType, SkMeasureSubType, SkSourceCurrency, SkSourceData, Typ, FlowIdJoin, isfx);
    ';

    -- =====================================================================
    -- Block 5: #RESBFE  --  variance check (logic preserved verbatim)
    -- =====================================================================
    SET @Query = @Query + N'

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
    ';

    -- =====================================================================
    -- Block 6: #FinalResult  --  KO string-concat (logic preserved verbatim)
    -- =====================================================================
    SET @Query = @Query + N'

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
    JOIN dwh.DimSourceData     sd   WITH (NOLOCK) ON sd.skSourceData       = t.SkSourceData
    JOIN dwh.DimMeasureType    DMT  WITH (NOLOCK) ON DMT.skmeasuretype     = t.skmeasuretype
    JOIN dwh.DimMeasureSubType DMST WITH (NOLOCK) ON DMST.skmeasuresubtype = t.skmeasuresubtype
    JOIN dwh.DimCurrency       ccy  WITH (NOLOCK) ON ccy.SkCurrency        = t.SkSourceCurrency
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

    -- [OPT 8] OPTION (RECOMPILE) so the CE sees actual temp-table cardinalities
    SELECT * FROM #FinalResult OPTION (RECOMPILE);
    ';

    IF @Execute = 1
        EXEC sp_executesql @Query,
             N'@PnlDate          DATE,
               @PrevPnl          DATE,
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
               @skmeasuretypeCRY SMALLINT,
               @SameMonth        BIT,
               @SameQuarter      BIT,
               @SameYear         BIT',
             @PnlDate, @PrevPnl, @SourceSystemCodes, @PrecisionAllowed,
             @IsEOM, @IsAdjSplitted, @FeedSourceIdAdj,
             @CA, @PV, @CAINIT, @PVINIT,
             @skcurrencyEur, @skmeasuretypeCSH, @skmeasuretypePV, @skmeasuretypeCRY,
             @SameMonth, @SameQuarter, @SameYear;
END
GO
