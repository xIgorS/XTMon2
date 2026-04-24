USE [LOG_FI_ALMT]
GO

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [monitoring]');
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [monitoring].[UspSystemDiagnosticsCleanLogging]
    @DeletedRows INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DELETE FROM [monitoring].[APSActionsLogs];
    SET @DeletedRows = @@ROWCOUNT;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [monitoring].[UspSystemDiagnosticsCleanHistory]
    @MonitoringLatestResultsDeleted INT OUTPUT,
    @MonitoringJobsDeleted INT OUTPUT,
    @JvCalculationJobResultsDeleted INT OUTPUT,
    @JvCalculationJobsDeleted INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF EXISTS (SELECT 1 FROM [monitoring].[MonitoringJobs] WHERE [Status] IN ('Queued', 'Running'))
       OR EXISTS (SELECT 1 FROM [monitoring].[JvCalculationJobs] WHERE [Status] IN ('Queued', 'Running'))
    BEGIN
        ;THROW 50001, 'Cannot clean history while monitoring or JV jobs are queued or running.', 1;
    END

    BEGIN TRANSACTION;

    DELETE FROM [monitoring].[MonitoringLatestResults];
    SET @MonitoringLatestResultsDeleted = @@ROWCOUNT;

    DELETE FROM [monitoring].[MonitoringJobs];
    SET @MonitoringJobsDeleted = @@ROWCOUNT;

    DELETE FROM [monitoring].[JvCalculationJobResults];
    SET @JvCalculationJobResultsDeleted = @@ROWCOUNT;

    DELETE FROM [monitoring].[JvCalculationJobs];
    SET @JvCalculationJobsDeleted = @@ROWCOUNT;

    COMMIT TRANSACTION;
END
GO