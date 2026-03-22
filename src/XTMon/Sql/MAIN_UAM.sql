USE [MAIN_UAM]
GO
/****** Object:  StoredProcedure [uam].[UspGetAdminUserByBnpId]    Script Date: 2/27/2026 6:26:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [uam].[UspGetAdminUserByBnpId]
	( @UserBnpId nvarchar(50), @IsTechnical bit = 0)
AS
BEGIN
	-- uam.UspGetAdminUserByBnpId 'GAIA\680098'
	return
	
	SELECT 
	1 UserId,
	@UserBnpId UserBnpId,
	'UName' FirstName,
	'ULName' LastName,
	'UName.ULName@domain.com' Email,
	11 RoleId,
	'APS' [Name]
    
END
GO
