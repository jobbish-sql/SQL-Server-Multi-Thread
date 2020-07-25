SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_Help
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_Help
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	Call this procedure to get T-SQL result sets that explain how to use the sp_AgentJobMultiThread framework.
*/

SELECT v.txt AS [Main Features]
FROM (
VALUES
(N'Create multiple threads to do work concurrently'),
(N'Stops all work at specified time limit'),
(N'Attempts to balance work among SQL Server schedulers')
) v (txt);


SELECT v.txt AS [Requirements]
FROM (
VALUES
	(N'SQL Server 2017+ or SQL Server 2016 SP2+'),
	(N'Standard, Developer, or Enterprise editions'),
	(N'Ability to create and run T-SQL Agent Jobs'),
	(N'VIEW_SERVER_STATE permission for the caller'),
	(N'SQLAgentUserRole or similar database role for the caller'),
	(N'db_datareader on the msdb database for the caller'),
	(N'execute procedure on this database for the caller'),
	(N'db_ddladmin, db_datawriter, db_datareader on the logging schema (@logging_schema_name) in the logging database (@logging_database_name) for the caller')
) v (txt);


SELECT v.txt AS [Creating a workload parent procedure]
FROM (
VALUES
	(N'A parent procedure serves as the application entry point for the sp_AgentJobMultiThread framework.'),
	(N'Reference the ThreadingDemoParent procedure in the demo folder for a simple example.'),
	(N'There are no required parameters.'),
	(N'Example code flow:'),
	(N'	STEP 1: Run standard validation by calling sp_AgentJobMultiThread_InitialValidation and quit if there are reported issues'),
	(N'	STEP 2: Run additional validation specific to to your workload and quit if there are reported issues'),
	(N'	STEP 3: Do setup work specific to your workload including creating and populated needed tables'),
	(N'	STEP 4: Create the agent jobs by calling sp_AgentJobMultiThread_CreateAgentJobs')
) v (txt);


SELECT v.txt AS [Creating a workload child procedure]
FROM (
VALUES
	(N'A child procedure serves as the application multithreading area for the sp_AgentJobMultiThread framework.'),
	(N'Reference the ThreadingDemoChild procedure in the demo folder for a simple example.'),
	(N'Parameters must exactly match the following:'),
	(N'	@logging_database_name SYSNAME,'),
	(N'	@logging_schema_name SYSNAME,)'),
	(N'	@parent_start_time DATETIME2,'),
	(N'	@job_number SMALLINT,'),
	(N'	@job_attempt_number SMALLINT'),
	(N'Example code flow:'),
	(N'	STEP 1: Do any prep work such as getting additional parameters from a summary table'),
	(N'	STEP 2: Call sp_AgentJobMultiThread_RescheduleChildJobIfNeeded and quit if the child procedure will be rescheduled'),
	(N'	STEP 3: In a loop, find the next unit of work'),
	(N'	STEP 4: In a loop, complete the unit of work'),
	(N'	STEP 5: In a loop, call sp_AgentJobMultiThread_ShouldChildJobHalt and quit if needed')
) v (txt);


SELECT v.txt AS [Creating a workload cleanup procedure]
FROM (
VALUES
	(N'A cleanup procedure serves as the application end point for the sp_AgentJobMultiThread framework.'),
	(N'Reference the ThreadingDemoCleanup procedure in the demo folder for a simple example.'),
	(N'Parameters must exactly match the following:'),
	(N'	@logging_database_name SYSNAME,'),
	(N'	@logging_schema_name SYSNAME,)'),
	(N'	@parent_start_time DATETIME2,'),
	(N'	@max_minutes_to_run SMALLINT'),
	(N'Example code flow:'),
	(N'	STEP 1: Do any prep work such as getting additional parameters from a summary table'),
	(N'	STEP 2: Call sp_AgentJobMultiThread_ShouldCleanupStopChildJobs to determine if cleanup should occur'),
	(N'	STEP 3: If cleanup should not occur yet then call sp_AgentJobMultiThread_FinalizeCleanup and quit'),
	(N'	STEP 4: If cleanup should occur then call sp_AgentJobMultiThread_CleanupChildJobs'),
	(N'	STEP 5: Complete any other necessary work in the procedure such as updating summary tables'),
	(N'	STEP 6: Call sp_AgentJobMultiThread_FinalizeCleanup to clean up agent jobs and schedules')
) v (txt);

RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_Internal_ValidateCommonParameters (
	@workload_identifier NVARCHAR(50),
	@logging_database_name SYSNAME,
	@logging_schema_name SYSNAME,
	@parent_start_time DATETIME2,
	@is_valid_OUT BIT OUTPUT,
	@error_message_OUT NVARCHAR(4000) OUTPUT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_Internal_ValidateCommonParameters
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	An internal procedure used to check common issues with parameters.
	You should not directly call this procedure.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@logging_database_name SYSNAME:

	Use this parameter if you want tables to be created in a different database than the database that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the database context of the sp_AgentJobMultiThread stored procedures.


@logging_schema_name SYSNAME:

	Use this parameter if you want tables to be created in a different schema than the schema that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the schema context of the sp_AgentJobMultiThread stored procedures.


@parent_start_time DATETIME2:

	The start time of the workload, preferably expressed in UTC.
	Use a consistent value for this value for each iteration of the workload for the parent, child, and cleanup procedures.


@is_valid_OUT BIT OUTPUT:

	Works with the @error_message_OUT @parameter.
	An output parameter used to report validation issues. If set to 1 then there is a problem that must be addressed.
	The caller should halt and the value for the output parameter @error_message_OUT should be returned to the client in some way.


@error_message_OUT NVARCHAR(4000) OUTPUT:

	Works with the is_valid_OUT @parameter.
	An output parameter used to report validation issues. If not null then there is a problem that must be addressed.
	The caller should halt and the value for the output parameter @error_message_OUT should be returned to the client in some way.
*/

DECLARE @dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@dynamic_sql_result_set_exists BIT;

SET NOCOUNT ON;

SET @is_valid_OUT = 0;


-- check @workload_identifier
IF @workload_identifier IS NULL
BEGIN
	SET @error_message_OUT = N'The @workload_identifier parameter must not be NULL.';
	RETURN;
END;


-- can't only pass in schema name
IF @logging_schema_name IS NOT NULL AND @logging_database_name IS NULL
BEGIN
	SET @error_message_OUT = N'If @logging_schema_name is not NULL then @logging_database_name must also not be NULL.';
	RETURN;
END;


-- check if logging database exists
IF @logging_database_name IS NOT NULL AND NOT EXISTS (
	SELECT 1
	FROM sys.databases
	WHERE name = @logging_database_name
)
BEGIN
	SET @error_message_OUT = QUOTENAME(@logging_database_name) + N' was entered as the @logging_database_name parameter but it does not exist as a database.'; 
	RETURN;
END;


IF @logging_schema_name IS NOT NULL
BEGIN
	-- validate logging schema exists
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @dynamic_sql_result_set_exists_OUT = 1
	FROM ' + QUOTENAME(@logging_database_name) + N'.sys.schemas
	WHERE name = @logging_schema_name';

	SET @dynamic_sql_result_set_exists = 0;
	EXEC sp_executesql @dynamic_sql_max,
	N'@logging_schema_name SYSNAME, @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
	@logging_schema_name = @logging_schema_name,
	@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

	IF @dynamic_sql_result_set_exists = 0
	BEGIN
		SET @error_message_OUT = N'Cannot find ' + QUOTENAME(@logging_schema_name) + N' schema in the ' + QUOTENAME(@logging_database_name) + N' database.'; 
		RETURN;
	END;
END;


-- check @parent_start_time
IF @parent_start_time IS NULL
BEGIN
	SET @error_message_OUT = N'The @parent_start_time parameter must not be NULL.';
	RETURN;
END;

-- all checks passed
SET @is_valid_OUT = 1;

RETURN;
END;

GO








CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_Internal_CheckProcedureExists (
	@procedure_name SYSNAME,
	@procedure_exists_OUT BIT OUTPUT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_Internal_CheckProcedureExists
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	An internal procedure used to check if a stored procedure exists.
	You should not directly call this procedure.


Parameter help:

@procedure_name SYSNAME:

	The name of the stored procedure to check existence for.
	This stored procedure must exist in the same database and schema as the sp_AgentJobMultiThread.

	
@procedure_exists_OUT BIT OUTPUT:

	An output parameter that reports if the stored procedure @procedure_name exists in the database and schema.
	Set to 1 if the procedure exists and set to 0 if the procedure does not exist.
*/

DECLARE @dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@procedure_exists BIT,
@code_database_name SYSNAME = DB_NAME(), -- all code objects are required to exist on the same database and schema
@code_schema_name SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID);


SET NOCOUNT ON;

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @procedure_exists_dynamic_OUT = 1
FROM ' + QUOTENAME(@code_database_name) + N'.sys.objects o
INNER JOIN ' + QUOTENAME(@code_database_name) + N'.sys.schemas s ON o.[schema_id] = s.[schema_id]
where o.name = @procedure_name
AND s.name = @code_schema_name
AND o.type = ''P''';

SET @procedure_exists_OUT = 0;
EXEC sp_executesql @dynamic_sql_max,
N'@procedure_name SYSNAME, @code_schema_name SYSNAME, @procedure_exists_dynamic_OUT BIT OUTPUT',
@procedure_name = @procedure_name,
@code_schema_name = @code_schema_name,
@procedure_exists_dynamic_OUT = @procedure_exists_OUT OUTPUT;  


RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_InitialValidation (
	@workload_identifier NVARCHAR(50),
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL,
	@parent_start_time DATETIME2,
	@child_stored_procedure_name SYSNAME,
	@cleanup_stored_procedure_name SYSNAME,
	@max_minutes_to_run SMALLINT,
	@total_jobs_to_create SMALLINT,
	@is_valid_OUT BIT OUTPUT,
	@error_message_OUT NVARCHAR(4000) OUTPUT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_InitialValidation
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the application workload parent procedure to verify permissions,
	that T-SQL agent jobs can be created, and to check for other common validation issues.
	Note that it is not possible to check for every issue.
	For example, a login might be sysadmin but still not be able to create T-SQL Agent jobs.

	It is important to check the value of the @is_valid_OUT and the @error_message_OUT parameters.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@logging_database_name SYSNAME:

	Use this parameter if you want tables to be created in a different database than the database that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the database context of the sp_AgentJobMultiThread stored procedures.


@logging_schema_name SYSNAME:

	Use this parameter if you want tables to be created in a different schema than the schema that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the schema context of the sp_AgentJobMultiThread stored procedures.


@parent_start_time DATETIME2:

	The start time of the workload, preferably expressed in UTC.
	Use a consistent value for this value for each iteration of the workload for the parent, child, and cleanup procedures.


@child_stored_procedure_name SYSNAME:

	The name of the stored procedure used to perform the "child" work for the workload.
	This stored procedure must exist in the same database and schema as the sp_AgentJobMultiThread.
	Reference sp_AgentJobMultiThread_Help if you need more information.


@cleanup_stored_procedure_name SYSNAME:

	The name of the stored procedure used to perform the "cleanup" work for the workload.
	This stored procedure must exist in the same database and schema as the sp_AgentJobMultiThread.
	Reference sp_AgentJobMultiThread_Help if you need more information.


@max_minutes_to_run SMALLINT:

	The maximum number of minutes the workload is allowed to run before all child jobs are stopped.


@total_jobs_to_create SMALLINT:

	The number of "child" jobs that will be created. Use this to set maximum concurrency of the workload.
	This is a required parameter with no default value. Going above the CPU count of the server is not recommended.


@is_valid_OUT BIT OUTPUT:

	Works with the @error_message_OUT @parameter.
	An output parameter used to report validation issues. If set to 1 then there is a problem that must be addressed.
	The caller should halt and the value for the output parameter @error_message_OUT should be returned to the client in some way.


@error_message_OUT NVARCHAR(4000) OUTPUT:

	Works with the is_valid_OUT @parameter.
	An output parameter used to report validation issues. If not null then there is a problem that must be addressed.
	The caller should halt and the value for the output parameter @error_message_OUT should be returned to the client in some way.
*/

DECLARE @product_version INT,
@dynamic_sql_max NVARCHAR(MAX),
@dynamic_sql_result_set_exists BIT,
@procedure_exists BIT,
@is_valid_common BIT,
@permission_check BIT,
@error_message_common NVARCHAR(4000),
@code_schema_name SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID);

SET NOCOUNT ON;


SET @is_valid_OUT = 0;


-- check for outer transaction or for implicit transactions
IF @@TRANCOUNT > 0
BEGIN
	SET @error_message_OUT = N'Cannot be called in an outer transaction because agent jobs may not start as expected.'; 
	RETURN;
END;


-- fail if on older version than 2016 SP2
-- it would likely be straightforward to make the agent framework work on SQL Server 2012 and 2014 but this has not bee tested
SET @product_version = TRY_CAST(PARSENAME(CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')), 4) AS INT);
IF @product_version < 13 OR (@product_version = 13 AND TRY_CAST(PARSENAME(CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')), 2) AS INT) < 5026)
BEGIN
	SET @error_message_OUT = N'Not tested on versions older than SQL Server 2012. Comment out this check in sp_AgentJobMultiThread_InitialValidation at your own risk to run on older versions.'; 
	RETURN;
END;


-- need the ability to run agent jobs so limit support to standard, developer, and enterprise
IF SERVERPROPERTY('EditionID') NOT IN (1804890536, 1872460670, 610778273, -2117995310, -1534726760)
BEGIN
	SET @error_message_OUT = N'Only supported on SQL Server Enterprise, Developer, and Standard editions.'; 
	RETURN;
END;


-- VIEW_SERVER_STATE permission is required
IF HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER STATE') = 0
BEGIN
	SET @error_message_OUT = N'VIEW_SERVER_STATE permission is required.'; 
	RETURN;
END;


-- SELECT permission on msdb database is required
IF HAS_PERMS_BY_NAME('msdb', N'DATABASE', N'SELECT') = 0
BEGIN
	SET @error_message_OUT = N'SELECT permission on msdb database is required.'; 
	RETURN;
END;


-- check if user can start agent jobs
-- docs say sysadmin or in the database roles SQLAgentUserRole, SQLAgentReaderRole, SQLAgentOperatorRole
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @permission_check_OUT = 1
FROM (
VALUES
	(IS_SRVROLEMEMBER(''sysadmin'')),
	(IS_ROLEMEMBER(''SQLAgentUserRole'')),
	(IS_ROLEMEMBER(''SQLAgentReaderRole'')),
	(IS_ROLEMEMBER(''SQLAgentOperatorRole''))
) v (perm)
HAVING SUM(v.perm) > 0';

SET @permission_check = 0;
-- must run in msdb context
EXEC msdb.sys.sp_executesql @dynamic_sql_max,
N'@permission_check_OUT BIT OUTPUT',
@permission_check_OUT = @permission_check OUTPUT;

IF @permission_check = 0
BEGIN
	SET @error_message_OUT = N'User cannot start T-SQL agent jobs due to lack of permissions.'; 
	RETURN;
END;


-- check if agent job service is running
IF EXISTS (
	SELECT 1
	FROM sys.dm_server_services dss
	WHERE dss.[servicename] LIKE N'SQL Server Agent (%'
	AND dss.[status] IS NULL OR dss.[status] <> 4
)
BEGIN
	SET @error_message_OUT = N'Agent job service is not running.'; 
	RETURN;
END;


-- use stored procedure schema name and database if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));


--  check EXECUTE on code schema
IF HAS_PERMS_BY_NAME(@code_schema_name, N'SCHEMA', N'EXECUTE') = 0
BEGIN
	SET @error_message_OUT = N'Executing user must have EXECUTE on the ' + QUOTENAME(@code_schema_name) + N' schema.';
	RETURN;
END;


-- check common parameters
EXEC [dbo].sp_AgentJobMultiThread_Internal_ValidateCommonParameters
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@is_valid_OUT = @is_valid_common OUTPUT,
	@error_message_OUT = @error_message_common OUTPUT;
	
IF @is_valid_common = 0
BEGIN
	SET @error_message_OUT = @error_message_common;
	RETURN;
END;


-- must have SELECT, INSERT, UPDATE, DELETE, and ALTER on the logging schema in the logging database
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'USE ' + QUOTENAME(@logging_database_name) + ';
	SELECT @dynamic_sql_result_set_exists_OUT = 1
	FROM (
	VALUES
		(HAS_PERMS_BY_NAME(''' + QUOTENAME(@logging_schema_name) + ''', N''SCHEMA'', N''SELECT'')),
		(HAS_PERMS_BY_NAME(''' + QUOTENAME(@logging_schema_name) + ''', N''SCHEMA'', N''INSERT'')),
		(HAS_PERMS_BY_NAME(''' + QUOTENAME(@logging_schema_name) + ''', N''SCHEMA'', N''UPDATE'')),
		(HAS_PERMS_BY_NAME(''' + QUOTENAME(@logging_schema_name) + ''', N''SCHEMA'', N''DELETE'')),
		(HAS_PERMS_BY_NAME(''' + QUOTENAME(@logging_schema_name) + ''', N''SCHEMA'', N''ALTER''))
	) v (perm)
	HAVING SUM(v.perm) = 5';
	
SET @dynamic_sql_result_set_exists = 0;
EXEC sys.sp_executesql @dynamic_sql_max,
N'@dynamic_sql_result_set_exists_OUT BIT OUTPUT',
@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;

IF @dynamic_sql_result_set_exists = 0
BEGIN
	SET @error_message_OUT = N'Executing user must have SELECT, INSERT, UPDATE, DELETE, and ALTER for the '
	+ QUOTENAME(@logging_schema_name) + N' schema in the ' + QUOTENAME(@logging_database_name) + N' database.'; 
	RETURN;
END;


-- validate @child_stored_procedure_name
EXEC [dbo].sp_AgentJobMultiThread_Internal_CheckProcedureExists
	@procedure_name = @child_stored_procedure_name,
	@procedure_exists_OUT = @procedure_exists OUTPUT;
	
IF @procedure_exists = 0
BEGIN
	SET @error_message_OUT = N'Value for @child_stored_procedure_name does not exist as a stored procedure in the expected database and schema.'; 
	RETURN;
END;


-- validate @cleanup_stored_procedure_name
EXEC [dbo].sp_AgentJobMultiThread_Internal_CheckProcedureExists
	@procedure_name = @cleanup_stored_procedure_name,
	@procedure_exists_OUT = @procedure_exists OUTPUT;
	
IF @procedure_exists = 0
BEGIN
	SET @error_message_OUT = N'Value for @cleanup_stored_procedure_name does not exist as a stored procedure in the expected database and schema.'; 
	RETURN;
END;


IF @max_minutes_to_run IS NULL OR @max_minutes_to_run <= 0
BEGIN
	SET @error_message_OUT = N'The @max_minutes_to_run parameter must be a positive number.';
	RETURN;
END;


IF @total_jobs_to_create IS NULL OR @total_jobs_to_create <= 0
BEGIN
	SET @error_message_OUT = N'The @total_jobs_to_create parameter must be a positive number.';
	RETURN;
END;


-- all checks passed
SET @is_valid_OUT = 1;

RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_CreateAgentJobs (
	@workload_identifier NVARCHAR(50),
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL,
	@parent_start_time DATETIME2,
	@child_stored_procedure_name SYSNAME,
	@cleanup_stored_procedure_name SYSNAME,
	@max_minutes_to_run SMALLINT,
	@job_prefix NVARCHAR(20) = NULL,
	@total_jobs_to_create SMALLINT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_CreateAgentJobs
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the application workload parent procedure to create the T-SQL agent jobs that call the child procedure.
	The owner of the agent jobs will be the executing user.

	The @child_stored_procedure_name stored procedure must exist in the same database and schema as this one and must have the following parameters:	
		@logging_database_name SYSNAME,
		@logging_schema_name SYSNAME,
		@parent_start_time DATETIME2,
		@job_number SMALLINT,
		@job_attempt_number SMALLINT

	The @cleanup_stored_procedure_name stored procedure must exist in the same database and schema as this one and must have the following parameters:
		@logging_database_name SYSNAME,
		@logging_schema_name SYSNAME,
		@parent_start_time DATETIME2,
		@max_minutes_to_run SMALLINT'

	For more information about child and cleanup procedures execute the sp_AgentJobMultiThread_Help stored procedure.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@logging_database_name SYSNAME:

	Use this parameter if you want tables to be created in a different database than the database that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the database context of the sp_AgentJobMultiThread stored procedures.


@logging_schema_name SYSNAME:

	Use this parameter if you want tables to be created in a different schema than the schema that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the schema context of the sp_AgentJobMultiThread stored procedures.


@parent_start_time DATETIME2:

	The start time of the workload, preferably expressed in UTC.
	Use a consistent value for this value for each iteration of the workload for the parent, child, and cleanup procedures.


@child_stored_procedure_name SYSNAME:

	The name of the stored procedure used to perform the "child" work for the workload.
	This stored procedure must exist in the same database and schema as the sp_AgentJobMultiThread.
	Reference sp_AgentJobMultiThread_Help if you need more information.


@cleanup_stored_procedure_name SYSNAME:

	The name of the stored procedure used to perform the "cleanup" work for the workload.
	This stored procedure must exist in the same database and schema as the sp_AgentJobMultiThread.
	Reference sp_AgentJobMultiThread_Help if you need more information. 


@max_minutes_to_run SMALLINT:

	The maximum number of minutes the workload is allowed to run before all child jobs are stopped.


@job_prefix NVARCHAR(20):

	An optional parameter that can be used to prepend an identifier to the beginning of agent jobs created by the framework.
	The default behavior is to prepend nothing to the agent jobs. They will start with the @workload_identifier parameter value.


@total_jobs_to_create SMALLINT:

	The number of "child" jobs that will be created. Use this to set maximum concurrency of the workload.
	This is a required parameter with no default value. Going above the CPU count of the server is not recommended.
*/


DECLARE @job_prefix_with_workload NVARCHAR(71),
@child_job_name SYSNAME,
@child_job_command NVARCHAR(4000),
@cleanup_job_name SYSNAME,
@cleanup_job_command NVARCHAR(4000),
@job_description NVARCHAR(512),
@current_time_local DATETIME2,
@cleanup_start_date_as_int INT,
@cleanup_start_time_as_int INT,
@child_job_counter SMALLINT,
@scheduler_table_name SYSNAME,
@stop_request_table_name SYSNAME,
@dynamic_sql_max NVARCHAR(MAX),
@dynamic_sql_result_set_exists BIT,
@procedure_exists BIT,
@is_valid_common BIT,
@error_message_common NVARCHAR(4000),
@code_database_name SYSNAME = DB_NAME(), -- all code objects are required to exist on the same database and schema
@code_schema_name SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID);

SET NOCOUNT ON;


-- validate parameters
-- check for outer transaction or for implicit transactions
IF @@TRANCOUNT > 0
BEGIN
	THROW 100000, N'Cannot call sp_AgentJobMultiThread_CreateAgentJobs in an outer transaction because agent jobs may not start as expected.', 1
	RETURN;
END;


-- use stored procedure schema name and database if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));

-- check common parameters
EXEC [dbo].sp_AgentJobMultiThread_Internal_ValidateCommonParameters
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@is_valid_OUT = @is_valid_common OUTPUT,
	@error_message_OUT = @error_message_common OUTPUT;
	
IF @is_valid_common = 0
BEGIN
	THROW 100010, @error_message_common, 1
	RETURN;
END;


-- validate @child_stored_procedure_name
SET @procedure_exists = 0;
EXEC [dbo].sp_AgentJobMultiThread_Internal_CheckProcedureExists
	@procedure_name = @child_stored_procedure_name,
	@procedure_exists_OUT = @procedure_exists OUTPUT;
	
IF @procedure_exists = 0
BEGIN
	THROW 100020, N'Value for @child_stored_procedure_name does not exist as a stored procedure in the expected database and schema.', 1; 
	RETURN;
END;


-- validate @cleanup_stored_procedure_name
SET @procedure_exists = 0;
EXEC [dbo].sp_AgentJobMultiThread_Internal_CheckProcedureExists
	@procedure_name = @cleanup_stored_procedure_name,
	@procedure_exists_OUT = @procedure_exists OUTPUT;
	
IF @procedure_exists = 0
BEGIN
	THROW 100025, N'Value for @cleanup_stored_procedure_name does not exist as a stored procedure in the expected database and schema.', 1; 
	RETURN;
END;

	
IF @max_minutes_to_run IS NULL OR @max_minutes_to_run <= 0
BEGIN
	THROW 100030, N'The @max_minutes_to_run parameter of sp_AgentJobMultiThread_CreateAgentJobs must be a positive number.', 1
	RETURN;
END;

	
IF @total_jobs_to_create IS NULL OR @total_jobs_to_create <= 0
BEGIN
	THROW 100050, N'The @total_jobs_to_create parameter of sp_AgentJobMultiThread_CreateAgentJobs must be a positive number.', 1
	RETURN;
END;


SET @job_prefix = ISNULL(N'', @job_prefix);
SET @job_prefix_with_workload = @job_prefix + @workload_identifier;

-- recreate *_Sessions_IDs_In_Use table so child jobs can try to get on their own schedulers
SET @scheduler_table_name = @workload_identifier + N'_Sessions_IDs_In_Use';

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'DROP TABLE IF EXISTS ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
+ N'.' + QUOTENAME(@scheduler_table_name);

EXEC sp_executesql @dynamic_sql_max;

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE TABLE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
+ N'.' + QUOTENAME(@scheduler_table_name) + N' (
	scheduler_id INT NOT NULL,
	session_id SMALLINT NOT NULL
	)';

EXEC sp_executesql @dynamic_sql_max;


-- create *_Last_Stop_Request table if it doesn't exist and insert a row
SET @stop_request_table_name = @workload_identifier + N'_Last_Stop_Request';

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT TOP (1) @dynamic_sql_result_set_exists_OUT = 1
FROM ' + QUOTENAME(@logging_database_name) + N'.sys.tables t
INNER JOIN ' + QUOTENAME(@logging_database_name) + N'.sys.schemas s ON t.[schema_id] = s.[schema_id]
where t.name = @stop_request_table_name
AND s.name = @logging_schema_name';

SET @dynamic_sql_result_set_exists = 0;
EXEC sp_executesql @dynamic_sql_max,
N'@dynamic_sql_result_set_exists_OUT BIT OUTPUT, @logging_schema_name SYSNAME, @stop_request_table_name SYSNAME',
@logging_schema_name = @logging_schema_name,
@stop_request_table_name = @stop_request_table_name,
@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

IF @dynamic_sql_result_set_exists = 0
BEGIN
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE TABLE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
	+ N'.' + QUOTENAME(@stop_request_table_name) + N' (
		Stop_Request_UTC DATETIME2 NOT NULL
	)';

	EXEC sp_executesql @dynamic_sql_max;

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
	+ N'.' + QUOTENAME(@stop_request_table_name) + N' WITH (TABLOCKX) (Stop_Request_UTC)
	VALUES(DATEADD(SECOND, -1, SYSUTCDATETIME()))';

	EXEC sp_executesql @dynamic_sql_max;
END;


-- delete any child jobs left around from previous runs
-- this can cause problems if a different user created the jobs last time, the jobs hit some kind of error, and the current
-- executing user doesn't have permission to delete the jobs.
-- I don't see a good way around this other than requiring possibly unacceptably high permissions for the calling user
DECLARE @child_jobs_to_delete TABLE (job_name sysname);

INSERT INTO @child_jobs_to_delete (job_name)
SELECT sj.name
FROM msdb.dbo.sysjobs sj
INNER JOIN msdb.dbo.sysjobactivity sja ON sj.job_id = sja.job_id
WHERE LEFT(sj.name, LEN(@job_prefix_with_workload)) = @job_prefix_with_workload
AND sj.name LIKE '%[_]Child[_]%'
OPTION (MAXDOP 1);

DECLARE jobs_to_delete CURSOR FOR   
SELECT job_name
FROM @child_jobs_to_delete;  

OPEN jobs_to_delete;  

FETCH NEXT FROM jobs_to_delete INTO @child_job_name;

WHILE @@FETCH_STATUS = 0  
BEGIN  
	EXEC msdb.dbo.sp_delete_job @job_name = @child_job_name;	

	FETCH NEXT FROM jobs_to_delete INTO @child_job_name;
END;
CLOSE jobs_to_delete;  
DEALLOCATE jobs_to_delete;  


-- create clean up job to halt the child jobs if needed
-- this should happen before the child jobs are created just in case something goes wrong
SET @cleanup_job_name = @job_prefix_with_workload + N'_Cleanup';

SET @cleanup_job_command  = N'EXEC ' + QUOTENAME(@code_schema_name) + N'.' + QUOTENAME(@cleanup_stored_procedure_name) + N' @logging_database_name=N''' + @logging_database_name
+ N''',
@logging_schema_name=N''' + @logging_schema_name
+ N''',
@parent_start_time=''' + CONVERT(NVARCHAR(30), @parent_start_time, 126)
+ N''',
@max_minutes_to_run = ' + CAST(@max_minutes_to_run AS NVARCHAR(5));

SET @job_description = N'Clean up job created for ' + @workload_identifier + N' workload by stored procedure sp_AgentJobMultiThread_CreateAgentJobs';

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @cleanup_job_name)
BEGIN	
	EXEC msdb.dbo.sp_delete_job @job_name = @cleanup_job_name;
END;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = @cleanup_job_name)
BEGIN	
	EXEC msdb.dbo.sp_delete_schedule @schedule_name = @cleanup_job_name, @force_delete = 1;
END;

EXEC msdb.dbo.sp_add_job @job_name = @cleanup_job_name,
@description = @job_description,
@delete_level = 0; -- cannot use delete level 1 when creating this job because it won't rerun

EXEC msdb.dbo.sp_add_jobstep @job_name = @cleanup_job_name,
@step_name = N'Clean up',
@command = @cleanup_job_command,
@database_name = @code_database_name;

EXEC msdb.dbo.sp_add_jobserver @job_name = @cleanup_job_name;

SET @current_time_local = SYSDATETIME();

-- :(
SET @cleanup_start_date_as_int = CAST(CONVERT(NVARCHAR(30), DATEADD(SECOND, 61, @current_time_local), 112) AS INT);
SET @cleanup_start_time_as_int = 10000 * DATEPART(HOUR, DATEADD(SECOND, 61, @current_time_local))
	+ 100 * DATEPART(MINUTE, DATEADD(SECOND, 61, @current_time_local))
	+ 1 * DATEPART(SECOND, DATEADD(SECOND, 61, @current_time_local));

EXEC msdb.dbo.sp_add_jobschedule @job_name = @cleanup_job_name,
@name = @cleanup_job_name,
@freq_type = 1,
@freq_interval = 1,
@active_start_date = @cleanup_start_date_as_int,
@active_start_time =  @cleanup_start_time_as_int;


-- create child jobs to do the concurrent work
SET @child_job_counter = 0;
WHILE @child_job_counter < @total_jobs_to_create
BEGIN
	SET @child_job_name = @job_prefix_with_workload + N'_Child_' + CAST(@child_job_counter AS NVARCHAR(5));

	-- create stored procedure call T-SQL job command for worker jobs
	SET @child_job_command  = N'EXEC ' + QUOTENAME(@code_schema_name) + N'.' + QUOTENAME(@child_stored_procedure_name) + N' @logging_database_name=N''' + @logging_database_name
	+ N''',
	@logging_schema_name=N''' + @logging_schema_name
	+ N''',
	@parent_start_time=''' + CONVERT(NVARCHAR(30), @parent_start_time, 126)
	+ N''',
	@job_number = ' + CAST(@child_job_counter AS NVARCHAR(5))
	+ N',
	@job_attempt_number = 1';

	SET @job_description = N'Child up job created for ' + @workload_identifier + N' workload by stored procedure sp_AgentJobMultiThread_CreateAgentJobs';

	EXEC msdb.dbo.sp_add_job @job_name = @child_job_name,
	@description = @job_description,
	@delete_level = 1;

	EXEC msdb.dbo.sp_add_jobstep @job_name = @child_job_name,
	@step_name = N'Do child work',
	@command = @child_job_command,
	@database_name = @code_database_name;

	EXEC msdb.dbo.sp_add_jobserver @job_name = @child_job_name;
	
	EXEC msdb.dbo.sp_start_job @job_name = @child_job_name;
	
	SET @child_job_counter = @child_job_counter + 1;
END;

RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_RescheduleChildJobIfNeeded (
	@workload_identifier NVARCHAR(50),
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL,
	@parent_start_time DATETIME2,
	@child_stored_procedure_name SYSNAME,
	@job_prefix NVARCHAR(20) = NULL,
	@job_number SMALLINT,
	@job_attempt_number SMALLINT,
	@max_reschedule_attempts SMALLINT = 25,
	@was_job_rescheduled_OUT BIT OUTPUT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_RescheduleChildJobIfNeeded
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the child workload procedure to determine if the agent job should halt
	because the framework has rescheduled it.

	It is important to check the value of the @was_job_rescheduled_OUT parameter.

	The @child_stored_procedure_name stored procedure must exist in the same database and schema as this one and must have the following parameters:	
		@logging_database_name SYSNAME,
		@logging_schema_name SYSNAME,
		@parent_start_time DATETIME2,
		@job_number SMALLINT,
		@job_attempt_number SMALLINT

	For more information about child procedures execute the sp_AgentJobMultiThread_Help stored procedure.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@logging_database_name SYSNAME:

	Use this parameter if you want tables to be created in a different database than the database that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the database context of the sp_AgentJobMultiThread stored procedures.


@logging_schema_name SYSNAME:

	Use this parameter if you want tables to be created in a different schema than the schema that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the schema context of the sp_AgentJobMultiThread stored procedures.


@parent_start_time DATETIME2:

	The start time of the workload, preferably expressed in UTC.
	Use a consistent value for this value for each iteration of the workload for the parent, child, and cleanup procedures.


@child_stored_procedure_name SYSNAME:

	The name of the stored procedure used to perform the "child" work for the workload.
	This stored procedure must exist in the same database and schema as the sp_AgentJobMultiThread.
	Reference sp_AgentJobMultiThread_Help if you need more information.


@job_prefix NVARCHAR(20):

	An optional parameter that can be used to prepend an identifier to the beginning of agent jobs created by the framework.
	The default behavior is to prepend nothing to the agent jobs. They will start with the @workload_identifier parameter value.


@job_number SMALLINT:

	Used to enumerate jobs to achieve job name uniqueness, allow for application logging, and reschedule jobs
	for CPU scheduler reasons as needed.	
	Will be present in the name of the agent job created.


@job_attempt_number SMALLINT:

	Used for internal purposes to reschedule jobs for CPU scheduler reasons as needed.	
	Will be present in the name of the agent job created.


@max_reschedule_attempts SMALLINT:

	The maximum amount of times a child job will delete and start itself in an attempt to get on its own CPU scheduler.
	The default value is 25.
	0 is an allowed value if the caller does not want there to be CPU rescheduling.
	Disabling may be a good idea if the child procedures do mostly parallel query work or the workload
	is running on a busy server.


@was_job_rescheduled_OUT BIT OUTPUT:

	An output parameter that reports if the agent job was rescheduled in an attempt to get on its own CPU scheduler.
	If this parameter is set to 0 then callers should allow the child procedure to continue.
	If this parameter is set to 1 then callers should halt the child procedure.
*/


DECLARE @scheduler_id INT,
@dynamic_sql_max NVARCHAR(MAX),
@was_row_inserted_into_sessions_table BIT,
@job_prefix_with_workload NVARCHAR(71),
@child_job_name SYSNAME,
@child_job_command NVARCHAR(4000),
@job_description NVARCHAR(512),
@scheduler_table_name SYSNAME,
@procedure_exists BIT,
@is_valid_common BIT,
@error_message_common NVARCHAR(4000),
@code_database_name SYSNAME = DB_NAME(), -- all code objects are required to exist on the same database and schema
@code_schema_name SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID);

SET NOCOUNT ON;


-- validate parameters
-- check for outer transaction or for implicit transactions
IF @@TRANCOUNT > 0
BEGIN
	THROW 100060, N'Cannot call sp_AgentJobMultiThread_RescheduleChildJobIfNeeded in an outer transaction because agent jobs may not start as expected.', 1
	RETURN;
END;


-- use stored procedure schema name and database if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));

-- check common parameters
EXEC [dbo].sp_AgentJobMultiThread_Internal_ValidateCommonParameters
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@is_valid_OUT = @is_valid_common OUTPUT,
	@error_message_OUT = @error_message_common OUTPUT;
	
IF @is_valid_common = 0
BEGIN
	THROW 100070, @error_message_common, 1
	RETURN;
END;


-- validate @child_stored_procedure_name
SET @procedure_exists = 0;
EXEC [dbo].sp_AgentJobMultiThread_Internal_CheckProcedureExists
	@procedure_name = @child_stored_procedure_name,
	@procedure_exists_OUT = @procedure_exists OUTPUT;
	
IF @procedure_exists = 0
BEGIN
	THROW 100080, N'Value for @child_stored_procedure_name does not exist as a stored procedure in the expected database and schema.', 1; 
	RETURN;
END;


IF @job_number IS NULL OR @job_number < 0
BEGIN
	THROW 100100, N'The @job_number parameter of sp_AgentJobMultiThread_RescheduleChildJobIfNeeded must be a non-negative number.', 1
	RETURN;
END;


IF @job_attempt_number IS NULL OR @job_attempt_number < 0
BEGIN
	THROW 100110, N'The @job_attempt_number parameter of sp_AgentJobMultiThread_RescheduleChildJobIfNeeded must be a non-negative number.', 1
	RETURN;
END;


IF @max_reschedule_attempts IS NULL OR @max_reschedule_attempts < 0
BEGIN
	THROW 100120, N'The @max_reschedule_attempts parameter of sp_AgentJobMultiThread_RescheduleChildJobIfNeeded must be a non-negative number.', 1
	RETURN;
END;


SET @job_prefix = ISNULL(N'', @job_prefix);
SET @job_prefix_with_workload = @job_prefix + @workload_identifier;
SET @scheduler_table_name = @workload_identifier + N'_Sessions_IDs_In_Use';

SELECT @scheduler_id = scheduler_id
FROM sys.dm_os_tasks
where session_id = @@SPID
AND exec_context_id = 0
OPTION (MAXDOP 1);

-- TABLOCKX hint to serialize, see no need for an application lock for this
SET @was_row_inserted_into_sessions_table = 0;
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'INSERT INTO ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
+ N'.' + QUOTENAME(@scheduler_table_name) + N' WITH (TABLOCKX)
(scheduler_id, session_id)
SELECT @scheduler_id, @@SPID
	WHERE NOT EXISTS (
	SELECT 1
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@scheduler_table_name) + N' s WITH (TABLOCKX) 
	WHERE s.scheduler_id = @scheduler_id
);

SET @was_row_inserted_into_sessions_table_OUT = @@ROWCOUNT';

EXEC sp_executesql @dynamic_sql_max,
N'@scheduler_id INT, @was_row_inserted_into_sessions_table_OUT BIT OUTPUT',
@scheduler_id = @scheduler_id,
@was_row_inserted_into_sessions_table_OUT = @was_row_inserted_into_sessions_table OUTPUT;

-- start a new job and quit if the job is sharing a scheduler and there are reattempts left
-- note that we log nothing to the scheduler table if we run out of reattempts. this should be ok given the long expected runtime of these procedures
-- in any case, if one of the procedures fails unexpectedly there isn't a clean way to clean out the row it inserted into the table
IF @was_row_inserted_into_sessions_table = 0 AND @job_attempt_number <= @max_reschedule_attempts
BEGIN
	SET @child_job_name = @job_prefix_with_workload + N'_Child_' + CAST(@job_number AS NVARCHAR(5)) + N'_Attempt_' + CAST(@job_attempt_number AS NVARCHAR(5));

	-- create stored procedure call T-SQL job command for worker jobs
	SET @child_job_command  = N'EXEC ' + QUOTENAME(@code_schema_name) + N'.' + QUOTENAME(@child_stored_procedure_name) + N' @logging_database_name=N''' + @logging_database_name
	+ N''',
	@logging_schema_name=N''' + @logging_schema_name
	+ N''',
	@parent_start_time=''' + CONVERT(NVARCHAR(30), @parent_start_time, 126)
	+ N''',
	@job_number = ' + CAST(@job_number AS NVARCHAR(5))
	+ N',
	@job_attempt_number = ' + CAST(@job_attempt_number + 1 AS NVARCHAR(5));

	SET @job_description = N'Child up job created for ' + @workload_identifier + N' workload by stored procedure sp_AgentJobMultiThread_CreateAgentJobs';

	EXEC msdb.dbo.sp_add_job @job_name = @child_job_name,
	@description = @job_description,
	@delete_level = 1;

	EXEC msdb.dbo.sp_add_jobstep @job_name = @child_job_name,
	@step_name = N'Do child work',
	@command = @child_job_command,
	@database_name = @code_database_name;

	EXEC msdb.dbo.sp_add_jobserver @job_name = @child_job_name;
	
	EXEC msdb.dbo.sp_start_job @job_name = @child_job_name;

	SET @was_job_rescheduled_OUT = 1;
	RETURN;
END;


SET @was_job_rescheduled_OUT = 0;
RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_ShouldChildJobHalt (
	@workload_identifier NVARCHAR(50),
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL,
	@parent_start_time DATETIME2,
	@should_job_halt_OUT BIT OUTPUT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_ShouldChildJobHalt
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the child workload procedure to determine if the stored procedure should halt.
	This is necessary because sometimes sp_stop_job will not stop an agent job.
	It is important to check the value of the @should_job_halt_OUT parameter.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@logging_database_name SYSNAME:

	Use this parameter if you want tables to be created in a different database than the database that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the database context of the sp_AgentJobMultiThread stored procedures.


@logging_schema_name SYSNAME:

	Use this parameter if you want tables to be created in a different schema than the schema that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the schema context of the sp_AgentJobMultiThread stored procedures.


@parent_start_time DATETIME2:

	The start time of the workload, preferably expressed in UTC.
	Use a consistent value for this value for each iteration of the workload for the parent, child, and cleanup procedures.

*/

DECLARE @dynamic_sql_max NVARCHAR(MAX);

SET NOCOUNT ON;

-- limited validation because this procedure may be called thousands of times
IF @workload_identifier IS NULL OR @parent_start_time IS NULL
BEGIN
	THROW 100130, N'The @workload_identifier and @parent_start_time parameters of sp_AgentJobMultiThread_ShouldChildJobHalt do not allow NULL.', 1
	RETURN;
END;

-- use stored procedure schema name and database if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));


SET @should_job_halt_OUT = 0;

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT TOP (1) @dynamic_sql_result_set_exists_OUT = 1
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
+ N'.CCI_Reorg_Rebuild_Last_Stop_Request WITH (TABLOCKX)
WHERE @parent_start_time <= Stop_Request_UTC';

EXEC sp_executesql @dynamic_sql_max,
N'@dynamic_sql_result_set_exists_OUT BIT OUTPUT, @parent_start_time DATETIME2',
@parent_start_time = @parent_start_time,
@dynamic_sql_result_set_exists_OUT = @should_job_halt_OUT OUTPUT;  

RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_ShouldCleanupStopChildJobs (
	@workload_identifier NVARCHAR(50),
	@parent_start_time DATETIME2,
	@job_prefix NVARCHAR(20) = NULL,
	@max_minutes_to_run SMALLINT,
	@should_stop_jobs_OUT BIT OUTPUT 
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_ShouldCleanupStopChildJobs
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the cleanup workload procedure to determine if the workload has either completed its work
	or hit the time limit.

	It is important to check the value of the @should_stop_jobs_OUT parameter to determine how the cleanup procedure should proceed.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@parent_start_time DATETIME2:

	The start time of the workload, preferably expressed in UTC.
	Use a consistent value for this value for each iteration of the workload for the parent, child, and cleanup procedures.


@job_prefix NVARCHAR(20):

	An optional parameter that can be used to prepend an identifier to the beginning of agent jobs created by the framework.
	The default behavior is to prepend nothing to the agent jobs. They will start with the @workload_identifier parameter value.


@max_minutes_to_run SMALLINT:

	The maximum number of minutes the workload is allowed to run before all child jobs are stopped.


@should_stop_jobs_OUT BIT OUTPUT:

	An output parameter that reports if the cleanup procedure should continue.
	The procedure checks if the time limit (see @max_minutes_to_run parameter) and checks if any child jobs are still running.
	If this parameter is set to 0 then callers should halt the cleanup procedure.
	If this parameter is set to 1 then callers should allow the cleanup procedure to continue.
*/

DECLARE @job_prefix_with_workload NVARCHAR(71);

SET NOCOUNT ON;

SET @should_stop_jobs_OUT = 0;

-- validate parameters
IF @workload_identifier IS NULL OR @parent_start_time IS NULL OR @max_minutes_to_run IS NULL
BEGIN
	THROW 100140, N'Only the @job_prefix parameter of sp_AgentJobMultiThread_ShouldCleanupStopChildJobs allows NULL.', 1
	RETURN;
END;


IF @max_minutes_to_run <= 0
BEGIN
	THROW 100150, N'The @max_minutes_to_run parameter of sp_AgentJobMultiThread_ShouldCleanupStopChildJobs must be a positive value.', 1
	RETURN;
END;


SET @job_prefix = ISNULL(N'', @job_prefix);
SET @job_prefix_with_workload = @job_prefix + @workload_identifier;
SET @should_stop_jobs_OUT = 0;

IF SYSUTCDATETIME() > DATEADD(MINUTE, @max_minutes_to_run, @parent_start_time)
BEGIN
	SET @should_stop_jobs_OUT = 1;
END
ELSE IF NOT EXISTS (
	SELECT sj.name
	FROM msdb.dbo.sysjobs sj
	INNER JOIN msdb.dbo.sysjobactivity sja ON sj.job_id = sja.job_id
	WHERE sja.start_execution_date IS NOT NULL
	AND sja.stop_execution_date IS NULL
	AND LEFT(sj.name, LEN(@job_prefix_with_workload)) = @job_prefix_with_workload
	AND sj.name <> @job_prefix_with_workload + N'_Cleanup'
	AND session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity)
)
BEGIN
	SET @should_stop_jobs_OUT = 1;
END;


RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_CleanupChildJobs (
	@workload_identifier NVARCHAR(50),
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL,
	@job_prefix NVARCHAR(20) = NULL,
	@child_job_error_count_OUT INT OUTPUT,
	@cleanup_error_message_OUT NVARCHAR(4000) OUTPUT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_CleanupChildJobs
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the cleanup workload procedure to stop any jobs that are still running.
	The output parameters provide useful troubleshooting information but they are not required to check.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@logging_database_name SYSNAME:

	Use this parameter if you want tables to be created in a different database than the database that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the database context of the sp_AgentJobMultiThread stored procedures.


@logging_schema_name SYSNAME:

	Use this parameter if you want tables to be created in a different schema than the schema that
	contains the sp_AgentJobMultiThread stored procedures.
	The default value is to use the schema context of the sp_AgentJobMultiThread stored procedures.


@job_prefix NVARCHAR(20):

	An optional parameter that can be used to prepend an identifier to the beginning of agent jobs created by the framework.
	The default behavior is to prepend nothing to the agent jobs. They will start with the @workload_identifier parameter value.


@child_job_error_count_OUT INT OUTPUT:

	An output parameter that reports the number of child jobs that reported some kind of error and the number of errors
	witnessed while stopping jobs.
	This parameter can be ignored by the caller but it's recommended to log this information somewhere for troubleshooting purposes.
	Note that this error count cannot be complete due to how logging works for agent jobs.
	Child job runtime errors can only be found if the child job quits on its own as opposed to being stopped.


@cleanup_error_message_OUT OUTPUT:

	An output parameter that contains an error message encountered while stopping jobs.
	This parameter can be ignored by the caller but it's recommended to log this information somewhere for troubleshooting purposes.
*/

DECLARE @job_prefix_with_workload NVARCHAR(71),
@job_name_to_stop SYSNAME,
@dynamic_sql_max NVARCHAR(MAX),
@stop_request_table_name SYSNAME,
@is_valid_common BIT,
@error_message_common NVARCHAR(4000);

SET NOCOUNT ON;


-- use stored procedure schema name and database if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));

-- validate parameters
-- check common parameters
EXEC [dbo].sp_AgentJobMultiThread_Internal_ValidateCommonParameters
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = '20200101', -- dummy value
	@is_valid_OUT = @is_valid_common OUTPUT,
	@error_message_OUT = @error_message_common OUTPUT;

IF @is_valid_common = 0
BEGIN
	THROW 100160, @error_message_common, 1
	RETURN;
END;


SET @job_prefix = ISNULL(N'', @job_prefix);
SET @job_prefix_with_workload = @job_prefix + @workload_identifier;
SET @child_job_error_count_OUT = 0;
SET @stop_request_table_name = @workload_identifier + N'_Last_Stop_Request';


-- log stop request in tables for child jobs to check if needed. sometimes jobs do not respect sp_stop_job calls
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name)
+ N'.' + QUOTENAME(@stop_request_table_name) + N' WITH (TABLOCKX)
SET Stop_Request_UTC = SYSUTCDATETIME()';

EXEC sp_executesql @dynamic_sql_max;


DECLARE @active_jobs TABLE (job_name sysname);

INSERT INTO @active_jobs (job_name)
SELECT sj.name
FROM msdb.dbo.sysjobs sj
INNER JOIN msdb.dbo.sysjobactivity sja ON sj.job_id = sja.job_id
WHERE sja.start_execution_date IS NOT NULL
AND sja.stop_execution_date IS NULL
AND LEFT(sj.name, LEN(@job_prefix_with_workload)) = @job_prefix_with_workload
AND sj.name <> @job_prefix_with_workload + N'_Cleanup'
AND session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity)
OPTION (MAXDOP 1);

DECLARE jobs_to_stop CURSOR FOR   
SELECT job_name
FROM @active_jobs;  

OPEN jobs_to_stop;  

FETCH NEXT FROM jobs_to_stop INTO @job_name_to_stop;

WHILE @@FETCH_STATUS = 0  
BEGIN  
	BEGIN TRY
		EXEC msdb.dbo.sp_stop_job @job_name = @job_name_to_stop;

		-- as far as I can tell if a job is stopped then any errors during execution are not recorded to any of the history tables
		-- it would be nice to not delete the job if there was an unexpected error in the child job
		EXEC msdb.dbo.sp_delete_job @job_name = @job_name_to_stop;
	END TRY
	BEGIN CATCH
		-- job might have stopped on its own before we could call stop, so ignore some errors:
		-- "The specified @job_name ('%') does not exist."
		-- "SQLServerAgent Error: Request to stop job % (from %) refused because the job is not currently running."
		IF ERROR_NUMBER() NOT IN (22022, 14262)
		BEGIN
			SET @child_job_error_count_OUT = @child_job_error_count_OUT + 1;
			SET @cleanup_error_message_OUT = ERROR_MESSAGE(); -- only keep one error, not worth the effort to get them all
		END;
	END CATCH;

	FETCH NEXT FROM jobs_to_stop INTO @job_name_to_stop;
END;

CLOSE jobs_to_stop;  
DEALLOCATE jobs_to_stop;  


-- add in errors from child jobs (these wouldn't have been stopped by the cursor)
SELECT @child_job_error_count_OUT = @child_job_error_count_OUT + COUNT_BIG(*)
FROM msdb.dbo.sysjobs sj
INNER JOIN msdb.dbo.sysjobhistory sjh ON sj.job_id = sjh.job_id
WHERE sjh.sql_message_id <> 0
AND LEFT(sj.name, LEN(@job_prefix)) = @job_prefix
AND sj.name <> @job_prefix_with_workload + N'_Cleanup';


RETURN;
END;

GO







CREATE OR ALTER PROCEDURE [dbo].sp_AgentJobMultiThread_FinalizeCleanup (
	@workload_identifier NVARCHAR(50),
	@job_prefix NVARCHAR(20) = NULL,
	@retry_cleanup BIT
)
AS
BEGIN
/*
Procedure Name: sp_AgentJobMultiThread_FinalizeCleanup
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 

	This stored procedure should be called by the cleanup workload procedure right before it quits.

	If @retry_cleanup = 1 then the cleanup will try again in about one minute.
	If @retry_cleanup = 0 then the cleanup will remove its agent job unless an error occurred during execution.


Parameter help:

@workload_identifier NVARCHAR(50):

	An identifier to use for the workload. This identifier will be added to any agent jobs and tables
	that are created by the framework. Use a consistent value for this identifier for the parent, child, and cleanup procedures.


@job_prefix NVARCHAR(20):

	An optional parameter that can be used to prepend an identifier to the beginning of agent jobs created by the framework.
	The default behavior is to prepend nothing to the agent jobs. They will start with the @workload_identifier parameter value.

@retry_cleanup BIT:
	
	Set this parameter to 1 if the cleanup procedure has done its work and there is no longer a need to run it.
	Set this parameter to 0 if the cleanup procedure halted early based on the output parameter of
	sp_AgentJobMultiThread_ShouldCleanupStopChildJobs and needs to try again in a minute.
*/

DECLARE @next_start_time_local DATETIME,
@cleanup_job_name SYSNAME,
@next_cleanup_start_date_int INT,
@next_cleanup_start_time_int INT,
@error_message NVARCHAR(4000)

SET NOCOUNT ON;


-- validate parameters
IF @workload_identifier IS NULL OR @retry_cleanup IS NULL
BEGIN
	THROW 100180, N'The @workload_identifier and @retry_cleanup parameters of sp_AgentJobMultiThread_FinalizeCleanup do not allow NULL.', 1
	RETURN;
END;

SET @job_prefix = ISNULL(N'', @job_prefix);
SET @cleanup_job_name = @job_prefix + @workload_identifier + N'_Cleanup';

IF @retry_cleanup = 0
BEGIN
	-- delete schedule since it is no longer needed
	BEGIN TRY
		EXEC msdb.dbo.sp_delete_schedule @schedule_name = @cleanup_job_name, @force_delete = 1;
	END TRY
	BEGIN CATCH
		-- can sometimes get into a state where an initial error results in errors rerunning this procedure, so ignore an error here if the step was already deleted
		-- The specified @schedule_name (%) does not exist.
		IF ERROR_NUMBER() NOT IN (14262)
		BEGIN
			SET @error_message = ERROR_MESSAGE();
			THROW 100190, @error_message, 1; 
		END;
	END CATCH;

	-- leave job around if there are errors
	EXEC msdb.dbo.sp_update_job @job_name=@cleanup_job_name, @delete_level=1;
END
ELSE
BEGIN
	-- change the schedule to run once again a minute from now. seemed easier than trying to set a schedule to run every minute
	SET @next_start_time_local = DATEADD(SECOND, 60, SYSDATETIME());

	SET @next_cleanup_start_date_int = CAST(CONVERT(NVARCHAR(30), @next_start_time_local, 112) AS INT);
	SET @next_cleanup_start_time_int = 10000 * DATEPART(HOUR, @next_start_time_local)
		+ 100 * DATEPART(MINUTE, @next_start_time_local)
		+ 1 * DATEPART(SECOND, @next_start_time_local);

	EXEC msdb.dbo.sp_update_schedule @name = @cleanup_job_name,
	@active_start_date = @next_cleanup_start_date_int,
	@active_start_time =  @next_cleanup_start_time_int;

END;

RETURN;
END;

GO
