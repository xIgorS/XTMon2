USE [STAGING_FI_ALMT]
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DROP PROCEDURE IF EXISTS [monitoring].[UspXtgMonitoringTechnicalRejectXtMonDelay];
DROP TABLE IF EXISTS [monitoring].[XtMonFunctionalRejectionDelayConfig];
GO

USE [DTM_FI]
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

DROP PROCEDURE IF EXISTS [monitoring].[UspXtgMonitoringTechnicalRejectXtMonDelay];
DROP TABLE IF EXISTS [monitoring].[XtMonFunctionalRejectionDelayConfig];
GO