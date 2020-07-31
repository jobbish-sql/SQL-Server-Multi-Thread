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
	@child_MAXDOP SMALLINT = NULL
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

	Simple example to show how to use the AgentJobMultiThread framework.
	This functions as the parent procedure for the workload.
	It performs any necessary prep work such as creating tables, populating queues, and creates the child jobs.	

	Minimum permissions required to run:
		* VIEW_SERVER_STATE
		* SQLAgentUserRole 
		* db_datareader on the msdb database
		* execute procedure on this database
		* db_ddladmin, db_datawriter, db_datareader on this database


Parameter help:

@max_minutes_to_run SMALLINT:

	The maximum number of minutes for the ThreadingDemo workload to run before all jobs are terminated.


@total_jobs_to_create SMALLINT:

	Specify the maximum number of concurrent child procedures that can be run.
	This is a required parameter with no default value. Going above the CPU count of the server is not recommended.


@child_MAXDOP SMALLINT:

	Set this to add a query level MAXDOP hint to code that executes in the child procedures.


*/

DECLARE @workload_identifier NVARCHAR(50) = N'ThreadingDemo',
@parent_start_time DATETIME2 = SYSUTCDATETIME(),
@child_stored_procedure_name SYSNAME = N'ThreadingDemoChild',
@cleanup_stored_procedure_name SYSNAME = N'ThreadingDemoCleanup',
@logging_schema_name SYSNAME,
@is_valid BIT,
@error_message NVARCHAR(4000);

SET NOCOUNT ON;

-- NOTE: All code and table objects should exist in the same database and schema.
-- This example code does not support putting tables in a different database or schema to improve readability.


-- use stored procedure schema
SET @logging_schema_name = OBJECT_SCHEMA_NAME(@@PROCID);


-- *** STEP 1: run standard validation ***
SET @is_valid = 1;
EXEC [dbo].AgentJobMultiThread_InitialValidation
	@workload_identifier = @workload_identifier,
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
IF NOT EXISTS (
	SELECT 1
	FROM sys.tables t
	INNER JOIN sys.schemas s ON t.[schema_id] = s.[schema_id]
	where t.name = N'ThreadingDemo_Summary'
	AND s.name = @logging_schema_name
)
BEGIN
	CREATE TABLE [dbo].ThreadingDemo_Summary (
		Summary_Start_Time_UTC DATETIME2 NOT NULL,
		Summary_End_Time_UTC DATETIME2 NULL,
		Total_Checksums BIGINT NULL,
		Maximum_Checksum_Value INT NULL,
		Max_Minutes_To_Run SMALLINT NOT NULL,
		Total_Jobs_To_Create SMALLINT NOT NULL,	
		Child_MAXDOP SMALLINT NULL,
		Approximate_Error_Count INT NULL,	
		Cleanup_Error_Text NVARCHAR(4000) NULL	
	);
	
	CREATE CLUSTERED INDEX CI_ThreadingDemo_Summary ON [dbo].ThreadingDemo_Summary (Summary_Start_Time_UTC);
END;

-- insert into summary table
INSERT INTO [dbo].ThreadingDemo_Summary (
  Summary_Start_Time_UTC	
, Max_Minutes_To_Run
, Total_Jobs_To_Create
, Child_MAXDOP
)
VALUES
(
  @parent_start_time
, @max_minutes_to_run
, @total_jobs_to_create
, @child_MAXDOP
);


-- drop and recreate a log table for the child jobs to report outcomes from their units of work
DROP TABLE IF EXISTS [dbo].ThreadingDemo_Child_Log;

CREATE TABLE [dbo].ThreadingDemo_Child_Log (
	Summary_Start_Time_UTC DATETIME2 NOT NULL,
	Job_Number SMALLINT NOT NULL,
	Batch_Id BIGINT NOT NULL,
	Checksum_Count BIGINT NOT NULL,
	Maximum_Checksum_Value INT NOT NULL,
	Query_Start_Time_UTC DATETIME2 NOT NULL,
	Query_Complete_Time_UTC DATETIME2 NULL,
	Session_Id INT NOT NULL,
	Error_Text NVARCHAR(4000) NULL
);


-- create a queue to hold units of work for the child jobs to pull from
-- ideally as much heavy lifting is done in the child jobs as possible compared to this procedure
DROP TABLE IF EXISTS [dbo].ThreadingDemo_Work_Queue;

CREATE TABLE [dbo].ThreadingDemo_Work_Queue (
	Batch_Id BIGINT
);


-- insert work to do into queue table
INSERT INTO [dbo].ThreadingDemo_Work_Queue WITH (TABLOCKX) (Batch_Id)
SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM master..spt_values t1
CROSS JOIN master..spt_values t2
OPTION (MAXDOP 1);



-- *** STEP 4: create the agent jobs ***
EXEC [dbo].AgentJobMultiThread_CreateAgentJobs
	@workload_identifier = @workload_identifier,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@cleanup_stored_procedure_name = @cleanup_stored_procedure_name,
	@max_minutes_to_run = @max_minutes_to_run,
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

	This functions as the child procedure for the workload.
	It gets work off a queue, performs that work, logs it to a table, and gets the next piece of work.
	The SELECT queries that do no useful work by design (they perform CHECKSUM and save off results).
	

Parameter help:

The parameters must exactly match what is required by the AgentJobMultiThread framework.
See the documentation for the framework for more information.

*/

DECLARE
@workload_identifier NVARCHAR(50) = N'ThreadingDemo',
@child_stored_procedure_name SYSNAME = N'ThreadingDemoChild',
@was_job_rescheduled BIT,
@dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@child_MAXDOP SMALLINT,
@error_message NVARCHAR(4000),
@all_available_work_complete BIT,
@batch_id BIGINT,
@checksum_count BIGINT,
@max_checksum_value INT,
@query_start_time DATETIME2,
@query_end_time DATETIME2,
@should_job_stop BIT,
@row_check BIT;

SET NOCOUNT ON;


-- *** STEP 1: if needed, get any needed additional parameters from a summary table ***
SELECT
    @row_check = 1
  , @child_MAXDOP = Child_MAXDOP 
FROM [dbo].ThreadingDemo_Summary
WHERE Summary_Start_Time_UTC = @parent_start_time;

   
-- there was likely a problem with the parent procedure if @row_check is NULL
IF @row_check IS NULL
BEGIN
	SET @error_message = N'Cannot find expected row in ThreadingDemo_Summary table. Look for an error logged by the ThreadingDemoParent stored procedure.';
	THROW 90020, @error_message, 1; 
	RETURN;
END;



-- *** STEP 2: check if procedure should quit due to rescheduling ***
SET @was_job_rescheduled = 0;
EXEC dbo.AgentJobMultiThread_RescheduleChildJobIfNeeded 
	@workload_identifier = @workload_identifier,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@job_number = @job_number,
	@job_attempt_number = @job_attempt_number,
	@was_job_rescheduled_OUT = @was_job_rescheduled OUTPUT;

IF @was_job_rescheduled = 1
BEGIN
	RETURN;
END;



-- *** STEP 3: find a unit of work ***
DECLARE @hold_deleted_row TABLE (
Batch_Id BIGINT NOT NULL
);

SET @should_job_stop = 0;
SET @all_available_work_complete = 0;
WHILE @all_available_work_complete = 0
BEGIN
	SET @batch_id = NULL;

	DELETE FROM @hold_deleted_row;

	DELETE TOP (1) FROM [dbo].ThreadingDemo_Work_Queue
	WITH (TABLOCKX)
	OUTPUT deleted.Batch_Id INTO @hold_deleted_row;

	SELECT @batch_id = Batch_Id
	FROM @hold_deleted_row;

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

		INSERT INTO [dbo].ThreadingDemo_Child_Log
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
		);



		-- *** STEP 5: call AgentJobMultiThread_ShouldChildJobHalt frequently ***
		EXEC [dbo].AgentJobMultiThread_ShouldChildJobHalt 
			@workload_identifier = @workload_identifier,
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

	This functions as the cleanup procedure for the workload.
	It calls APIs to remove any currently running jobs.
	It also writes to a summary table which is custom code for this workload.

Parameter help:

The parameters must exactly match what is required by the AgentJobMultiThread framework.
See the documentation for the framework for more information.

*/

DECLARE @workload_identifier NVARCHAR(50) = N'ThreadingDemo',
@stop_jobs BIT,
@error_message NVARCHAR(4000),
@error_count_from_cleanup INT,
@error_message_from_cleanup NVARCHAR(4000),
@row_check BIT;

SET NOCOUNT ON;


-- *** STEP 1: if needed, get any needed additional parameters from a summary table ***
SELECT
    @row_check = 1
FROM [dbo].ThreadingDemo_Summary
WHERE Summary_Start_Time_UTC = @parent_start_time;

   
-- there was likely a problem with the parent procedure if @row_check is NULL
IF @row_check IS NULL
BEGIN
	SET @error_message = N'Cannot find expected row in ThreadingDemo_Summary table. Look for an error logged by the ThreadingDemoParent stored procedure.';
	THROW 90030, @error_message, 1; 
	RETURN;
END;



SET @stop_jobs = 0;
-- *** STEP 2: call AgentJobMultiThread_ShouldCleanupStopChildJobs to determine if jobs should be stopped ***
EXEC [dbo].AgentJobMultiThread_ShouldCleanupStopChildJobs
	@workload_identifier = @workload_identifier,
	@parent_start_time = @parent_start_time,
	@max_minutes_to_run = @max_minutes_to_run,
	@should_stop_jobs_OUT = @stop_jobs OUTPUT;



-- *** STEP 3: call AgentJobMultiThread_FinalizeCleanup to reschedule job if cleanup can't happen yet ***
IF @stop_jobs = 0
BEGIN
	EXEC [dbo].AgentJobMultiThread_FinalizeCleanup
		@workload_identifier = @workload_identifier,
		@retry_cleanup = 1;

	RETURN;
END;


-- *** STEP 4: call AgentJobMultiThread_CleanupChildJobs to clean up jobs ***
EXEC [dbo].AgentJobMultiThread_CleanupChildJobs
	@workload_identifier = @workload_identifier,
	@child_job_error_count_OUT = @error_count_from_cleanup OUTPUT,
	@cleanup_error_message_OUT = @error_message_from_cleanup OUTPUT;



-- *** STEP 5: do any other necessary work in the procedure, such as updating a summary table ***
-- update summary table
UPDATE summary
SET Summary_End_Time_UTC = SYSUTCDATETIME()
, Total_Checksums = log_results.Total_Checksums
, Maximum_Checksum_Value = log_results.Maximum_Checksum_Value
, Approximate_Error_Count = log_results.Child_Error_Count + @error_count_from_cleanup
, Cleanup_Error_Text = @error_message_from_cleanup
FROM [dbo].ThreadingDemo_Summary summary
CROSS JOIN
(
	SELECT
	  SUM(1.0 * l.Checksum_Count) Total_Checksums
	, MAX(l.Maximum_Checksum_Value) Maximum_Checksum_Value
	, COUNT_BIG(l.Error_Text) Child_Error_Count
	FROM [dbo].ThreadingDemo_Child_Log l 
	WHERE l.Summary_Start_Time_UTC = @parent_start_time
) log_results
WHERE summary.Summary_Start_Time_UTC = @parent_start_time;


-- purge rows from permanent tables older than 100 days
DELETE FROM [dbo].ThreadingDemo_Summary
WITH (TABLOCK)
WHERE Summary_Start_Time_UTC < DATEADD(DAY, -100, SYSUTCDATETIME());



-- *** STEP 6: call AgentJobMultiThread_FinalizeCleanup to clean up unnecessary jobs and schedules ***
EXEC [dbo].AgentJobMultiThread_FinalizeCleanup
	@workload_identifier = @workload_identifier,
	@retry_cleanup = 0;

RETURN;
END;

GO
