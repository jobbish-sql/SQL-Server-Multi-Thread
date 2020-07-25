SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO


CREATE OR ALTER PROCEDURE [dbo].ThreadingDemoParent (
	@max_minutes_to_run SMALLINT,
	@total_jobs_to_create SMALLINT,
	@child_MAXDOP SMALLINT = NULL,
	@job_prefix NVARCHAR(20) = NULL,
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL
)
AS
BEGIN
/*
Procedure Name: ThreadingDemoParent
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 


Parameter help:

*/

DECLARE @workload_identifier NVARCHAR(50) = N'ThreadingDemo',
@parent_start_time DATETIME2 = SYSUTCDATETIME(),
@child_stored_procedure_name SYSNAME = N'ThreadingDemoChild',
@cleanup_stored_procedure_name SYSNAME = N'ThreadingDemoCleanup',
@is_valid BIT,
@error_message NVARCHAR(4000),
@used_job_prefix NVARCHAR(20) = ISNULL(@job_prefix, N''),
@dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@dynamic_sql_result_set_exists BIT,
@queue_table_name SYSNAME,
@summary_table_name SYSNAME,
@summary_table_index_name SYSNAME,
@child_log_table_name SYSNAME;

SET NOCOUNT ON;

-- TODO: read through all code


-- NOTE: All code objects should exist in the same database and schema.
-- Tables can exist somewhere else by setting the @logging_database_name and @logging_schema_name parameters.


SET @summary_table_name = @workload_identifier + N'_Summary';
SET @summary_table_index_name = N'CI_' + @workload_identifier + N'_Summary'
SET @queue_table_name = @workload_identifier + N'_Work_Queue';
SET @child_log_table_name = @workload_identifier + N'_Child_Log';

-- use stored procedure name and database if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));


-- *** STEP 1: run standard validation ***
SET @is_valid = 1;
EXEC [dbo].sp_AgentJobMultiThread_InitialValidation
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@cleanup_stored_procedure_name = @cleanup_stored_procedure_name,
	@max_minutes_to_run = @max_minutes_to_run,
	@total_jobs_to_create = @total_jobs_to_create,
	@is_valid_OUT = @is_valid OUTPUT,
	@error_message_OUT = @error_message OUTPUT;
	
IF @is_valid = 0
BEGIN
	THROW 90000, @error_message, 1;
	RETURN;
END;



-- *** STEP 2: run custom validation specific to this procedure ***
IF @child_MAXDOP < 0
BEGIN
	THROW 90010, N'@child_MAXDOP parameter, if set, must be a non-negative integer', 1;
	RETURN;
END;



-- *** STEP 3: do setup work including creating and populating needed tables ***

-- create a summary table if it doesn't exist
-- summary tables are useful to store statistics from the run as well as to pass additional parameters to the child procedure
-- adding the parameters to the command text directly can introduce a SQL injection risk
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @dynamic_sql_result_set_exists_OUT = 1
FROM ' + QUOTENAME(@logging_database_name) + N'.sys.tables t
INNER JOIN ' + QUOTENAME(@logging_database_name) + N'.sys.schemas s ON t.[schema_id] = s.[schema_id]
where t.name = @summary_table_name
AND s.name = @logging_schema_name';

SET @dynamic_sql_result_set_exists = 0;
EXEC sp_executesql @dynamic_sql_max,
N'@dynamic_sql_result_set_exists_OUT BIT OUTPUT, @summary_table_name SYSNAME, @logging_schema_name SYSNAME',
@logging_schema_name = @logging_schema_name,
@summary_table_name = @summary_table_name,
@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

IF @dynamic_sql_result_set_exists = 0
BEGIN
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE TABLE ' + QUOTENAME(@logging_database_name)
	+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name) + N' (
		Summary_Start_Time_UTC DATETIME2 NOT NULL,
		Summary_End_Time_UTC DATETIME2 NULL,
		Total_Checksums BIGINT NULL,
		Maximum_Checksum_Value INT NULL,
		Max_Minutes_To_Run SMALLINT NOT NULL,
		Used_Job_Prefix NVARCHAR(20) NOT NULL,	
		Total_Jobs_To_Create SMALLINT NOT NULL,	
		Child_MAXDOP SMALLINT NULL,
		Approximate_Error_Count INT NULL,	
		Cleanup_Error_Text NVARCHAR(4000) NULL	
	);
	
	CREATE CLUSTERED INDEX ' + QUOTENAME(@summary_table_index_name) + N' ON '
	+ QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name) + N' (Summary_Start_Time_UTC)';

	EXEC sp_executesql @dynamic_sql_max;
END;

-- insert into summary table
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name) + N' (
  Summary_Start_Time_UTC	
, Max_Minutes_To_Run
, Used_Job_Prefix
, Total_Jobs_To_Create
, Child_MAXDOP
)
VALUES
(
  @parent_start_time
, @max_minutes_to_run
, @used_job_prefix
, @total_jobs_to_create
, @child_MAXDOP
)';

EXEC sp_executesql @dynamic_sql_max,
N'@parent_start_time DATETIME2, @max_minutes_to_run SMALLINT, @used_job_prefix NVARCHAR(20), @total_jobs_to_create SMALLINT, @child_MAXDOP SMALLINT',
@parent_start_time = @parent_start_time,
@max_minutes_to_run = @max_minutes_to_run,
@used_job_prefix = @used_job_prefix,
@total_jobs_to_create = @total_jobs_to_create,
@child_MAXDOP = @child_MAXDOP;


-- drop and recreate a log table for the child jobs to report outcomes from their units of work
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'DROP TABLE IF EXISTS ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@child_log_table_name);

EXEC sp_executesql @dynamic_sql_max;

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE TABLE ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@child_log_table_name) + N' (
	Summary_Start_Time_UTC DATETIME2 NOT NULL,
	Job_Number SMALLINT NOT NULL,
	Batch_Id BIGINT NOT NULL,
	Checksum_Count BIGINT NOT NULL,
	Maximum_Checksum_Value INT NOT NULL,
	Query_Start_Time_UTC DATETIME2 NOT NULL,
	Query_Complete_Time_UTC DATETIME2 NULL,
	Session_Id INT NOT NULL,
	Error_Text NVARCHAR(4000) NULL
)';

EXEC sp_executesql @dynamic_sql_max;


-- create a queue to hold units of work for the child jobs to pull from
-- ideally as much heavy lifting is done in the child jobs as possible compared to this procedure
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'DROP TABLE IF EXISTS ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@queue_table_name);

EXEC sp_executesql @dynamic_sql_max;

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE TABLE ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@queue_table_name) + N' (
	Batch_Id BIGINT
)';

EXEC sp_executesql @dynamic_sql_max;


-- insert work to do into queue table
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@queue_table_name) + N' WITH (TABLOCKX) (Batch_Id)
	SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	FROM master..spt_values t1
	CROSS JOIN master..spt_values t2
	OPTION (MAXDOP 1)';

EXEC sp_executesql @dynamic_sql_max;



-- *** STEP 4: create the agent jobs ***
EXEC [dbo].sp_AgentJobMultiThread_CreateAgentJobs
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@cleanup_stored_procedure_name = @cleanup_stored_procedure_name,
	@max_minutes_to_run = @max_minutes_to_run,
	@job_prefix = @used_job_prefix,
	@total_jobs_to_create = @total_jobs_to_create;


RETURN;
END;

GO










CREATE OR ALTER PROCEDURE [dbo].ThreadingDemoChild (
	@logging_database_name SYSNAME,
	@logging_schema_name SYSNAME,
	@parent_start_time DATETIME2,
	@job_number SMALLINT,
	@job_attempt_number SMALLINT
)
AS
BEGIN
/*
Procedure Name: ThreadingDemoChild
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 


Parameter help:

*/

DECLARE
@workload_identifier NVARCHAR(50) = N'ThreadingDemo',
@child_stored_procedure_name SYSNAME = N'ThreadingDemoChild',
@was_job_rescheduled BIT,
@used_job_prefix NVARCHAR(20),
@queue_table_name SYSNAME,
@summary_table_name SYSNAME,
@child_log_table_name SYSNAME,
@dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@child_MAXDOP SMALLINT,
@error_message NVARCHAR(4000),
@all_available_work_complete BIT,
@batch_id BIGINT,
@checksum_count BIGINT,
@max_checksum_value INT,
@query_start_time DATETIME2,
@query_end_time DATETIME2,
@should_job_stop BIT;

SET NOCOUNT ON;

SET @queue_table_name = @workload_identifier + N'_Work_Queue';
SET @summary_table_name = @workload_identifier + N'_Summary';
SET @child_log_table_name = @workload_identifier + N'_Child_Log';



-- *** STEP 1: if needed, get any needed additional parameters from a summary table ***
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'SELECT
    @used_job_prefix_OUT = Used_Job_Prefix
  , @child_MAXDOP_OUT = Child_MAXDOP 
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name)
+ N' WHERE Summary_Start_Time_UTC = @parent_start_time';

EXEC sp_executesql @dynamic_sql_max,
N'@used_job_prefix_OUT NVARCHAR(20) OUTPUT, @child_MAXDOP_OUT SMALLINT OUTPUT, @parent_start_time DATETIME2',
@used_job_prefix_OUT = @used_job_prefix OUTPUT,
@child_MAXDOP_OUT = @child_MAXDOP OUTPUT,
@parent_start_time = @parent_start_time;
   
-- there was likely a problem with the parent procedure if @used_job_prefix is NULL
IF @used_job_prefix IS NULL
BEGIN
	SET @error_message = N'Cannot find expected row in ' + @summary_table_name + N' table. Look for an error logged by the ThreadingDemoParent stored procedure.';
	THROW 60000, @error_message, 1; 
	RETURN;
END;



-- *** STEP 2: check if procedure should quit due to rescheduling ***
SET @was_job_rescheduled = 0;
EXEC dbo.sp_AgentJobMultiThread_RescheduleChildJobIfNeeded 
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@job_prefix = @used_job_prefix,
	@job_number = @job_number,
	@job_attempt_number = @job_attempt_number,
	@max_reschedule_attempts = 25,
	@was_job_rescheduled_OUT = @was_job_rescheduled OUTPUT;

IF @was_job_rescheduled = 1
BEGIN
	RETURN;
END;



-- *** STEP 3: find a unit of work ***
SET @should_job_stop = 0;
SET @all_available_work_complete = 0;
WHILE @all_available_work_complete = 0
BEGIN
	SET @batch_id = NULL;

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'DECLARE @hold_deleted_row TABLE (
	Batch_Id BIGINT NOT NULL
	);

	DELETE TOP (1) FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@queue_table_name) + N'
	WITH (TABLOCKX)
	OUTPUT deleted.Batch_Id INTO @hold_deleted_row;

	SELECT @batch_id_OUT = Batch_Id
	FROM @hold_deleted_row';

	EXEC sp_executesql @dynamic_sql_max,
	N'@batch_id_OUT BIGINT OUTPUT',
	@batch_id_OUT = @batch_id OUTPUT;

	-- if NULL then there is no more work to do
	IF @batch_id IS NULL
	BEGIN
		SET @all_available_work_complete = 1;
	END
	ELSE
	BEGIN
		-- *** STEP 4: complete the unit of work ***
		SET @error_message = NULL;
		SET @query_start_time = SYSUTCDATETIME();

		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'SELECT
		  @checksum_count_OUT = COUNT_BIG(*)
		, @max_checksum_value_OUT = MAX(CHECKSUM(CAST(@batch_id AS BIGINT) + t1.[number] + t2.[number]))
		FROM master..spt_values t1
		CROSS JOIN master..spt_values t2'
		+ CASE WHEN @child_MAXDOP IS NOT NULL THEN N' OPTION(MAXDOP ' + CAST(@child_MAXDOP AS NVARCHAR(5)) + N')' ELSE N'' END;

		BEGIN TRY
			EXEC sp_executesql @dynamic_sql_max,
			N'@checksum_count_OUT BIGINT OUTPUT, @max_checksum_value_OUT INT OUTPUT, @batch_id INT',
			@checksum_count_OUT = @checksum_count OUTPUT,
			@max_checksum_value_OUT = @max_checksum_value OUTPUT,
			@batch_id = @batch_id;
		END TRY
		BEGIN CATCH
			SET @error_message = ERROR_MESSAGE();		
		END CATCH;

		SET @query_end_time = SYSUTCDATETIME();

		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO ' + QUOTENAME(@logging_database_name)
		+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@child_log_table_name) + N'
		(
		  Summary_Start_Time_UTC
		, Job_Number
		, Batch_Id
		, Checksum_Count
		, Maximum_Checksum_Value
		, Query_Start_Time_UTC
		, Query_Complete_Time_UTC
		, Session_Id
		, Error_Text
		)		
		VALUES (
		  @parent_start_time
		, @job_number
		, @batch_id
		, @checksum_count
		, @max_checksum_value
		, @query_start_time
		, @query_end_time
		, @@SPID
		, @error_message
		)';

		EXEC sp_executesql @dynamic_sql_max,
		N'@parent_start_time DATETIME2, @job_number SMALLINT, @batch_id BIGINT, @checksum_count BIGINT, @max_checksum_value INT, @query_start_time DATETIME2, @query_end_time DATETIME2, @error_message NVARCHAR(4000)',
		@parent_start_time = @parent_start_time,
		@job_number = @job_number,
		@batch_id = @batch_id,
		@checksum_count = @checksum_count,
		@max_checksum_value = @max_checksum_value,
		@query_start_time = @query_start_time,
		@query_end_time = @query_end_time,
		@error_message = @error_message;



		-- *** STEP 5: call sp_AgentJobMultiThread_ShouldChildJobHalt frequently ***
		EXEC [dbo].sp_AgentJobMultiThread_ShouldChildJobHalt 
			@workload_identifier = @workload_identifier,
			@logging_database_name = @logging_database_name,
			@logging_schema_name = @logging_schema_name,
			@parent_start_time = @parent_start_time,
			@should_job_halt_OUT = @should_job_stop OUTPUT;

		IF @should_job_stop = 1
		BEGIN
			SET @all_available_work_complete = 1;
		END;
	END;
END;

RETURN;
END;

GO










CREATE OR ALTER PROCEDURE [dbo].ThreadingDemoCleanup (
	@logging_database_name SYSNAME,
	@logging_schema_name SYSNAME,
	@parent_start_time DATETIME2,
	@max_minutes_to_run SMALLINT
)
AS
BEGIN
/*
Procedure Name: ThreadingDemoCleanup
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: 


Parameter help:



*/

DECLARE @workload_identifier NVARCHAR(50) = N'ThreadingDemo',
@stop_jobs BIT,
@used_job_prefix NVARCHAR(20),
@dynamic_sql_max NVARCHAR(MAX),
@summary_table_name SYSNAME,
@child_log_table_name SYSNAME,
@error_message NVARCHAR(4000),
@error_count_from_cleanup INT,
@error_message_from_cleanup NVARCHAR(4000);

SET NOCOUNT ON;

SET @summary_table_name = @workload_identifier + N'_Summary';
SET @child_log_table_name = @workload_identifier + N'_Child_Log';


-- *** STEP 1: if needed, get any needed additional parameters from a summary table ***
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'SELECT
    @used_job_prefix_OUT = Used_Job_Prefix
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name)
+ N' WHERE Summary_Start_Time_UTC = @parent_start_time';

EXEC sp_executesql @dynamic_sql_max,
N'@used_job_prefix_OUT NVARCHAR(20) OUTPUT, @parent_start_time DATETIME2',
@used_job_prefix_OUT = @used_job_prefix OUTPUT,
@parent_start_time = @parent_start_time;
   
-- there was likely a problem with the parent procedure if @used_job_prefix is NULL
IF @used_job_prefix IS NULL
BEGIN
	SET @error_message = N'Cannot find expected row in ' + @summary_table_name + N' table. Look for an error logged by the ThreadingDemoParent stored procedure.';
	THROW 60000, @error_message, 1; 
	RETURN;
END;



SET @stop_jobs = 0;
-- *** STEP 2: call sp_AgentJobMultiThread_ShouldCleanupStopChildJobs to determine if jobs should be stopped ***
EXEC [dbo].sp_AgentJobMultiThread_ShouldCleanupStopChildJobs
	@workload_identifier = @workload_identifier,
	@parent_start_time = @parent_start_time,
	@job_prefix = @used_job_prefix,
	@max_minutes_to_run = @max_minutes_to_run,
	@should_stop_jobs_OUT = @stop_jobs OUTPUT;



-- *** STEP 3: call sp_AgentJobMultiThread_FinalizeCleanup to reschedule job if cleanup can't happen yet ***
IF @stop_jobs = 0
BEGIN
	EXEC [dbo].sp_AgentJobMultiThread_FinalizeCleanup
		@workload_identifier = @workload_identifier,
		@job_prefix = @used_job_prefix,
		@retry_cleanup = 1;

	RETURN;
END;


-- *** STEP 4: call sp_AgentJobMultiThread_CleanupChildJobs to clean up jobs ***
EXEC [dbo].sp_AgentJobMultiThread_CleanupChildJobs
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@job_prefix = @used_job_prefix,
	@child_job_error_count_OUT = @error_count_from_cleanup OUTPUT,
	@cleanup_error_message_OUT = @error_message_from_cleanup OUTPUT;



-- *** STEP 5: do any other necessary work in the procedure, such as updating a summary table ***

-- update summary table
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'UPDATE summary
SET Summary_End_Time_UTC = SYSUTCDATETIME()
, Total_Checksums = log_results.Total_Checksums
, Maximum_Checksum_Value = log_results.Maximum_Checksum_Value
, Approximate_Error_Count = log_results.Child_Error_Count + @error_count_from_cleanup
, Cleanup_Error_Text = @error_message_from_cleanup
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name) + N' summary
CROSS JOIN
(
	SELECT
	  SUM(1.0 * l.Checksum_Count) Total_Checksums
	, MAX(l.Maximum_Checksum_Value) Maximum_Checksum_Value
	, COUNT_BIG(l.Error_Text) Child_Error_Count
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@child_log_table_name) + N' l 
	WHERE l.Summary_Start_Time_UTC = @parent_start_time
) log_results
WHERE summary.Summary_Start_Time_UTC = @parent_start_time';

EXEC sp_executesql @dynamic_sql_max,
N'@error_count_from_cleanup INT, @error_message_from_cleanup NVARCHAR(4000), @parent_start_time DATETIME2',
@error_count_from_cleanup = @error_count_from_cleanup,
@error_message_from_cleanup = @error_message_from_cleanup,
@parent_start_time = @parent_start_time;


-- purge rows from permanent tables older than 100 days
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'DELETE FROM ' + QUOTENAME(@logging_database_name)
+ N'.' + QUOTENAME(@logging_schema_name) + N'.' + QUOTENAME(@summary_table_name)
+ N' WITH (TABLOCK)
WHERE Summary_Start_Time_UTC < DATEADD(DAY, -100, SYSUTCDATETIME())';

EXEC sp_executesql @dynamic_sql_max;



-- *** STEP 6: call sp_AgentJobMultiThread_FinalizeCleanup to clean up unnecessary jobs ***
EXEC [dbo].sp_AgentJobMultiThread_FinalizeCleanup
	@workload_identifier = @workload_identifier,
	@job_prefix = @used_job_prefix,
	@retry_cleanup = 0;

RETURN;
END;

GO
