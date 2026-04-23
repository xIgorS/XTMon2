USE [LOG_FI_ALMT]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[monitoring].[JvCalculationJobs]', N'U') IS NOT NULL
BEGIN
    IF EXISTS
    (
        SELECT 1
        FROM sys.check_constraints
        WHERE name = N'CK_JvCalculationJobs_Status'
          AND parent_object_id = OBJECT_ID(N'[monitoring].[JvCalculationJobs]')
    )
    BEGIN
        ALTER TABLE [monitoring].[JvCalculationJobs]
            DROP CONSTRAINT [CK_JvCalculationJobs_Status];
    END

    ALTER TABLE [monitoring].[JvCalculationJobs] WITH CHECK
        ADD CONSTRAINT [CK_JvCalculationJobs_Status]
        CHECK ([Status] IN ('Queued', 'Running', 'Completed', 'Failed', 'Cancelled'));

    ALTER TABLE [monitoring].[JvCalculationJobs]
        CHECK CONSTRAINT [CK_JvCalculationJobs_Status];
END
GO