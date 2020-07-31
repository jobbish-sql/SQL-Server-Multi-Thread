SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO

-- previous version used sp_ prefix. that was a mistake
DROP PROCEDURE IF EXISTS dbo.sp_CCIReorgAndRebuild;

GO

CREATE OR ALTER PROCEDURE [dbo].[CCIReorgAndRebuild] (
	@CCI_included_database_name_list NVARCHAR(4000),
	@CCI_excluded_schema_name_list NVARCHAR(4000) = NULL,
	@CCI_excluded_table_name_list NVARCHAR(4000) = NULL,
	@max_CCI_alter_job_count SMALLINT,
	@max_minutes_to_run SMALLINT,
	@partition_priority_algorithm_name NVARCHAR(100) = N'DEFAULT',
	@SQL_expression_for_partition_priority_calculation NVARCHAR(4000) = NULL,
	@rebuild_algorithm_name NVARCHAR(100) = N'NEVER',
	@SQL_expression_for_rebuild_calculation NVARCHAR(4000) = NULL,
	@ignore_archive_compressed_partitions BIT = 1,
	@reorg_use_COMPRESS_ALL_ROWGROUPS_option BIT = 1,
	@reorg_execute_twice BIT = 0,
	@rebuild_MAXDOP SMALLINT = NULL,
	@rebuild_ONLINE_option BIT = 0,
	@start_stored_procedure_name_to_run SYSNAME = NULL,
	@end_stored_procedure_name_to_run SYSNAME = NULL,
	@logging_database_name SYSNAME = NULL,
	@logging_schema_name SYSNAME = NULL,
	@disable_CPU_rescheduling BIT = 0,
	@delimiter_override NVARCHAR(1) = NULL,
	@job_prefix_override NVARCHAR(20) = NULL,
	@prioritization_only BIT = 0
)
AS
BEGIN
/*
Procedure Name: CCIReorgAndRebuild
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose:

	A maintenance solution to do ALTER TABLE ... REORGANIZE and REBUILD on clustered columnstore indexes. 
	Designed to run on large servers against very large databases.
	Main features:
		* Can create multiple threads to do work concurrently
		* Stops all work at time limit
		* Supports columnstore indexes in multiple databases
		* Over 40 data points available to define the priority order at the partition level for maintenance actions
		* Over 40 data points available to choose between REORGANIZE and REBUILD at the partition level
		* Saves history of previous runs which can be used for prioritization
		* Queries against sys.dm_db_column_store_row_group_physical_stats run multi-threaded and skipped if possible
		* Attempts to balance work among schedulers


	Key Limitations: 
		* Only runs on SQL Server 2016 SP2+ and 2017+.
		* Requires the ability to creates T-SQL Agent Jobs.
		* Only performs maintenance actions against clustered columnstore indexes.
		* Does not respect segment level ordering within a partition which can be ruined by REORGANIZE and REBUILD. Such tables should be excluded.
		* REBUILD is only performable at the partition level instead of at the table level for partitioned tables.


	Minimum permissions required to run:
		* VIEW_SERVER_STATE
		* SQLAgentUserRole 
		* db_datareader on the msdb database
		* execute procedure on this database
		* db_ddladmin, db_datawriter, db_datareader on the logging schema @logging_schema_name in the logging database @logging_database_name
		* db_ddladmin on all databases included in @CCI_included_database_name_list
		
	WARNING: the @SQL_expression_for_partition_priority_calculation and @SQL_expression_for_rebuild_calculation allow for SQL injection.
	This is unavoidable due to what the parameters are used to (passing a SQL expression to be run as part of a more complex query).
	If this is unacceptable to you set the @Disable_SQL_Expression_Parameters local variable in this procedure to 1.

Troubleshooting help:

	This procedure creates T-SQL agent jobs so troubleshooting when something goes wrong can be difficult.
	Consider using the @prioritization_only parameter to check everything out before running it for real.
	Outside of the T-SQL agent job history, looking at the following tables may be helpful:

		[CCI_Reorg_Rebuild_Summary] - contains one row per run of the maintenance solution
		[CCI_Reorg_Rebuild_Index_History] -- contains one row per attempted maintenance action on a partition
		[CCI_Reorg_Rebuild_Partitions_To_Process] -- contains one row per CCI partition that haven't had a completed maintenance action for the current run


Parameter help:

@logging_database_name SYSNAME:

	The name of the database that contains all of the tables used by this CCI maintenance solution.
	This does not need to match the database that contains all of the stored procedures.
	Required procedures:
		[CCIReorgAndRebuild]
		[CCI_Reorg_Rebuild_Child_Job]
		[CCI_Reorg_Rebuild_Cleanup_Jobs]

	Required tables:
		[CCI_Reorg_Rebuild_Summary]
		[CCI_Reorg_Rebuild_Index_History]


@logging_schema_name SYSNAME:

	The schema name that contains all of the tables used by this CCI maintenance solution.
	This does not need to match the schema that contains all of the stored procedures.
	See documentation for the @logging_database_name parameter.


@CCI_included_database_name_list NVARCHAR(4000):

	A list of databases to search for columnstore indexes which could benefit from maintenance operations.
	By default, this is comma delimited.
	Example usage: "database_1,database_2,database_3".
	The delimiter can be changed with the @delimiter_override parameter.


@CCI_excluded_schema_name_list NVARCHAR(4000):

	A list of schemas used to exclude columnstore indexes from the search for columnstore indexes
	which could benefit from maintenance operations. By default, this is comma delimited.
	Example usage: "schema_1,schema_2,schema_3".
	The delimiter can be changed with the @delimiter_override parameter.


@CCI_excluded_table_name_list NVARCHAR(4000):

	A list of tables used to exclude columnstore indexes from the search for columnstore indexes
	which could benefit from maintenance operations. By default, this is comma delimited.
	Example usage: "table_1,table_2,table_3".
	The delimiter can be changed with the @delimiter_override parameter.

	
@max_CCI_alter_job_count SMALLINT:

	Specify the maximum number of concurrent ALTER INDEX statements that can be run.
	This is a required parameter with no default value. Going above the CPU count of the server is not recommended.
	One important thing to note is that ALTER INDEX... REORGANIZE by default gets the maximum possible query memory grant.
	That means that concurrency may be effectively limited at 3 for systems without TF 6404 or
	without Resource Governor limiting the maximum query memory grant.


@max_minutes_to_run SMALLINT:

	The maximum number of minutes for the columnstore maintenance solution to run before all jobs are terminated.


@partition_priority_algorithm_name NVARCHAR(100):

	Choose the algorithm to prioritize the order of index maintenance operations against columnstore indexes. Choices are:
	
		DEFAULT - uses the default algorithm set in this procedure. This may change over time. Current logic:	
			A) ignore any partition with less than 1000 rows
			B) first process partitions with deleted + closed + open >= 100000 in order of percentage fragmentation DESC
			C) then process partitions which haven't had a maintenance operation for 30 days in order of
			"compressed rowgroup count under 500k rows"/"rowgroup count" DESC
			D) then process partitions which haven't had a maintenance operation for 30 days in order of size DESC
			E) then process partitions with deleted + closed + open >= 1000 in order of percentage fragmentation DESC
			F) then process partitions in order of "compressed rowgroup count under 500k rows"/"rowgroup count" DESC
			G) ignore any other partitions

		CUSTOM_PRESET - uses an end user defined SQL expression. You must set a value for the
		@SQL_expression_for_partition_priority_calculation_CUSTOM_PRESET variable in this procedure to use this option. 

		CUSTOM_USE_PARAMETER_EXPRESSION - uses the value supplied in the @SQL_expression_for_partition_priority_calculation parameter to do prioritization.


@SQL_expression_for_partition_priority_calculation NVARCHAR(4000):

	A SQL expression used to set the priority order of index maintenance operations. Priority is set at the partition level and most
	columns in the CCI_Reorg_Rebuild_Partitions_To_Process table can be used to calculate the priority. This expression must evaluate to a value
	that can be cast to a NUMERIC(38, 0) for all partitions. Anything with a priority calculated as zero will not be processed.
	This parameter should not be set unless @partition_priority_algorithm_name = N'CUSTOM_USE_PARAMETER_EXPRESSION'.
	Note that using a column prefixed with cci_ requires a query against sys.dm_db_column_store_row_group_physical_stats which can be prohibitively expensive on some systems.

	Examples to show acceptable syntax:
		
		-- skip partitions with no rows, otherwise prioritize partitions that were stopped last time that weren't able to run for the full window, otherwise prioritize by least recent to be processed
		CASE WHEN cci_part_row_count = 0 THEN 0
		WHEN alter_last_attempt_status_flag = 3 THEN 987654321987654321987654321
		WHEN alter_last_complete_time IS NULL THEN 987654321987654321
		ELSE DATEDIFF(HOUR, alter_last_complete_time, SYSUTCDATETIME())
		END

		-- skip any partition not in the four right most partitions, otherwise order by number of not compressed rows in a partition
		CASE WHEN sql_part_distance_from_rightmost_partition >= 4 THEN 0
		ELSE cci_part_open_row_count + cci_part_deleted_row_count + cci_part_closed_row_count + cci_part_tombstone_row_count
		END

	
	Here is the full list of columns available in the CCI_Reorg_Rebuild_Partitions_To_Process table along with some explanations:

		Database_Name SYSNAME NOT NULL,
		[Schema_Name] SYSNAME NOT NULL,
		Table_Name SYSNAME NOT NULL,
		Index_Name SYSNAME NOT NULL,
		Partition_Number INT NOT NULL, -- set to -1 for unpartitioned tables
		Database_Id INT NOT NULL,
		Object_Id INT NOT NULL,
		maxdop_limit SMALLINT NULL, -- does not account for size of partition, only looks at database and system settings
		partition_is_archive_compressed BIT NULL,
		alter_last_attempt_time_utc DATETIME2 NULL, -- the time a partition was last processed by this maintenance solution with any outcome
		alter_last_attempt_status_flag TINYINT NULL, -- 1 for completion, 2 for stopped after running for entire maintenance window, 3 for stopped after running for part of maintenance window, 4 for error
		alter_last_attempt_was_rebuild BIT NULL, -- 1 if the most recent maintenance action for a partition was a REBUILD
		alter_last_complete_time DATETIME2 NULL, -- most recent time a maintenance operation for a partition was successful
		alter_last_partial_timeout_time_utc DATETIME2 NULL, -- most recent time a maintenance operation for a partition timed out after running for part of the maintenance window
		alter_last_full_timeout_time_utc DATETIME2 NULL, -- most recent time a maintenance operation for a partition timed out after running for the full window
		alter_last_error_time DATETIME2 NULL, -- most recent time a maintenance operation for a partition had an error
		sql_part_approx_row_count BIGINT DEFAULT 0, -- number of rows from sys.dm_db_partition_stats
		sql_part_approx_bytes BIGINT DEFAULT 0, -- number of used bytes from sys.dm_db_partition_stats
		sql_table_approx_row_count BIGINT DEFAULT 0,
		sql_table_approx_bytes BIGINT DEFAULT 0,
		sql_part_distance_from_rightmost_partition INT DEFAULT 0, -- set to 1 for the right most partition, 2 for the second right most partition, etc
		cci_part_row_count BIGINT DEFAULT 0, -- number of rows from sys.dm_db_column_store_row_group_physical_stats
		cci_part_open_row_count BIGINT DEFAULT 0,
		cci_part_compressed_row_count BIGINT DEFAULT 0,
		cci_part_deleted_row_count BIGINT DEFAULT 0,
		cci_part_closed_row_count BIGINT DEFAULT 0,
		cci_part_tombstone_row_count BIGINT DEFAULT 0,
		cci_part_dict_pressure_row_count BIGINT DEFAULT 0,
		cci_part_memory_pressure_row_count BIGINT DEFAULT 0,
		cci_part_rowgroup_count BIGINT DEFAULT 0, -- number of rowgroups from sys.dm_db_column_store_row_group_physical_stats
		cci_part_open_rowgroup_count BIGINT DEFAULT 0,
		cci_part_compressed_rowgroup_count BIGINT DEFAULT 0,
		cci_part_closed_rowgroup_count BIGINT DEFAULT 0,
		cci_part_tombstone_rowgroup_count BIGINT DEFAULT 0,
		cci_part_compressed_rowgroup_count_under_17k_rows BIGINT DEFAULT 0,
		cci_part_compressed_rowgroup_count_under_132k_rows BIGINT DEFAULT 0,
		cci_part_compressed_rowgroup_count_under_263k_rows BIGINT DEFAULT 0,
		cci_part_compressed_rowgroup_count_under_525k_rows BIGINT DEFAULT 0,
		cci_part_dict_pressure_rowgroup_count BIGINT DEFAULT 0,
		cci_part_memory_pressure_rowgroup_count BIGINT DEFAULT 0,
		cci_part_approx_deleted_rows_bytes BIGINT DEFAULT 0, -- a guess at bytes used by deleted rows using sys.dm_db_column_store_row_group_physical_stats
		cci_part_approx_compressed_bytes BIGINT DEFAULT 0,
		cci_part_approx_uncompressed_bytes BIGINT DEFAULT 0,
		cci_part_total_bytes BIGINT DEFAULT 0,
		cci_table_total_bytes BIGINT DEFAULT 0, -- total bytes for the table using sys.dm_db_column_store_row_group_physical_stats
		cci_table_row_count BIGINT DEFAULT 0, -- total rows for the table using sys.dm_db_column_store_row_group_physical_stats
		cci_table_open_row_count BIGINT DEFAULT 0,
		cci_table_compressed_row_count BIGINT DEFAULT 0,
		cci_table_deleted_row_count BIGINT DEFAULT 0,
		cci_table_closed_row_count BIGINT DEFAULT 0,
		cci_table_tombstone_row_count BIGINT DEFAULT 0,

	
@rebuild_algorithm_name NVARCHAR(100):

	Choose the algorithm to determine if an index operation should be a REBUILD or a REORGANIZE. Choices are:

		DEFAULT - uses the default algorithm set in this procedure. This may change over time. Current logic:	
			A) don't do rebuild if the last maintenance operation on the partition was a rebuild that was stopped after running for the full maintenance window
			B) do a rebuild if the partition has at least 8 million rows and the ratio of deleted rows to total rows exceeds 1.6 / "available maxdop"

		CUSTOM_PRESET - uses an end user defined SQL expression. You must set a value for the
		@SQL_expression_for_rebuild_calculation_CUSTOM_PRESET variable in this procedure to use this option. 

		CUSTOM_USE_PARAMETER_EXPRESSION - uses the value supplied in the @SQL_expression_for_rebuild_calculation to do prioritization.

		NEVER - all index operations use REORGANIZE.

		ALWAYS - all index operations use REBUILD.


@SQL_expression_for_rebuild_calculation NVARCHAR(4000):

	A SQL expression used to determine if a partition should undergo a REBUILD or a REORGANIZE. The value of this expression is cast to a BIT
	so anything > 0 will be considered to be a REBUILD. This parameter should not be set unless @rebuild_algorithm_name = N'CUSTOM_USE_PARAMETER_EXPRESSION'.
	Note that using a column prefixed with cci_ requires a query against sys.dm_db_column_store_row_group_physical_stats which can be prohibitively expensive on some systems.
	
	Example to show acceptable syntax:
	
		-- rebuild anything with at least 4 million deleted rows if available MAXDOP >= 4 and the partition is not archive compressed:
		CASE WHEN cci_part_deleted_row_count > 4000000 AND maxdop_limit >= 4 AND partition_is_archive_compressed = 0 THEN 1 ELSE 0 END

	Reference the @SQL_expression_for_partition_priority_calculation parameter to see a list of available columns to use for the calculation.


@ignore_archive_compressed_partitions BIT:

	Exclude archive compressed partitions from the search for indexes that could benefit from maintenance. By default these are excluded. If set to 0 you can use the partition_is_archive_compressed column to prioritize archive compressed partitions differently from partitions with standard columnstore compression.


@reorg_use_COMPRESS_ALL_ROWGROUPS_option BIT:

	Controls the COMPRESS_ALL_ROW_GROUP option for ALTER INDEX... REORGANIZE. With the default value of this parameter, REORGANIZE statements will run with COMPRESS_ALL_ROW_GROUP = ON. Change that by setting this parameter to 0.


@reorg_execute_twice BIT:

	ALTER INDEX... REORGANIZE does not always result in a table or partition without any fragmentation. For example, TOMBSTONE rowgroups may be left behind after a REORGANIZE. Set this parameter to 1 to immediately run another REORGANIZE on the table or partition after the first has completed. rowgroups. This can be helpful for some workloads in that it will immediately remove the TOMBSTONE rowgroups.


@rebuild_MAXDOP SMALLINT:

	Set this parameter to set the MAXDOP option for ALTER INDEX... REBUILD. By default the MAXDOP option won't be passed to the REBUILD statement. The actual DOP of the REBUILD can be impacted by many things, including MAXDOP settings at any level, cardinality estimates for the partition, and expected memory usage needed to compress the data.


@rebuild_ONLINE_option BIT:

	Controls the ONLINE option for ALTER INDEX... REBUILD which was introduced by Microsoft with SQL Server 2019. With the default value of this parameter, REBUILD statements will run with ONLINE = OFF. Note that rebuilds even with ONLINE = OFF are "partially online". The data in the partition can be read by not modified.


@start_stored_procedure_name_to_run SYSNAME:

	Specify the name of a stored procedure that exists on the current stored procedure database and in the stored procedure schema to run at the start of maintenance. This can be used to quiesce an application. If the stored procedure throws an error then maintenance will not run.


@end_stored_procedure_name_to_run SYSNAME:
	
	Specify the name of a stored procedure that exists on the current stored procedure database and in the stored procedure schema to run at the end of maintenance during cleanup.


@disable_CPU_rescheduling BIT:

	ALTER INDEX... REORGANIZE always runs with MAXDOP 1. It can be helpful for overall throughput to try to nudge long running,
	MAXDOP 1 processes onto their own schedulers. The maintenance solution checks the scheduler assigned to the child jobs
	and restarts the child job if another child job is already running on that scheduler. Child jobs will restart up to 25 times
	to try to get on their own schedulers. This behavior is disabled by setting this parameter to 1.


@delimiter_override NVARCHAR(1):

	Set this parameter to change the default delimiter from a comma to something else for the @CCI_included_database_name_list, @CCI_excluded_schema_name_list, and @CCI_excluded_table_name_list parameters.


@job_prefix_override NVARCHAR(20):

	This stored procedure creates agent jobs with a prefix of "CCI_Reorg_Rebuild" by default. Set this parameter if you need to append additional characters to that prefix for any reason.


@prioritization_only BIT:

	Setting this parameter to 1 runs the maintenance solution in test mode. Prioritization of all partitions is set but no ALTER INDEX... statements are run. This mode can be useful for verifying that no errors are thrown and validating that the partition priority and rebuild algorithm calculations are working as expected. The following query can be useful for that type of analysis:

		SELECT *
		FROM CCI_Reorg_Rebuild_Partitions_To_Process
		ORDER BY String_Priority_Helper DESC

*/



-- IMPORTANT: set the @Disable_SQL_Expression_Parameters variable to 1 if you want to disable
-- the @SQL_expression_for_partition_priority_calculation  and @SQL_expression_for_rebuild_calculation parameters
DECLARE @Disable_SQL_Expression_Parameters BIT = 0;

-- IMPORTANT: set the variable below if you wish to use the CUSTOM_PRESET option for the @partition_priority_algorithm_name parameter
DECLARE @SQL_expression_for_partition_priority_calculation_CUSTOM_PRESET NVARCHAR(4000) = NULL;

-- IMPORTANT: set the variable below if you wish to use the CUSTOM_PRESET option for @rebuild_algorithm_name parameter
DECLARE @SQL_expression_for_rebuild_calculation_CUSTOM_PRESET NVARCHAR(4000) = NULL;



DECLARE @workload_identifier NVARCHAR(50) = N'CCI_Reorg_Rebuild',
@child_stored_procedure_name SYSNAME = N'CCI_Reorg_Rebuild_Child_Job',
@cleanup_stored_procedure_name SYSNAME = N'CCI_Reorg_Rebuild_Cleanup_Jobs',
@code_database_name SYSNAME,
@code_schema_name SYSNAME,
@parm_definition NVARCHAR(4000),
@dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@view_text NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@view_name_with_schema NVARCHAR(400),
@dynamic_sql_result_set_exists BIT = 0,
@is_valid_initial BIT,
@parent_start_time DATETIME2 = SYSUTCDATETIME(),
@product_version INT,
@database_count BIGINT,
@actual_database_count BIGINT,
@nice_error_message NVARCHAR(4000),
@current_CCI_database_name SYSNAME,
@current_CCI_database_id INT,
@delimiter NVARCHAR(1) = ISNULL(@delimiter_override, N','),
@query_part_level_info BIT = 0,
@query_CCI_DMV_info BIT = 0,
@query_history_table BIT = 0,
@SQL_expression_for_partition_priority_calculation_DEFAULT NVARCHAR(4000),
@SQL_expression_for_rebuild_calculation_DEFAULT NVARCHAR(4000),
@SQL_expression_for_rebuild_calculation_NEVER NVARCHAR(4000),
@SQL_expression_for_rebuild_calculation_ALWAYS NVARCHAR(4000),
@used_SQL_expression_for_partition_priority_calculation NVARCHAR(4000),
@used_SQL_expression_for_rebuild_calculation NVARCHAR(4000),
@job_prefix NVARCHAR(20) = ISNULL(@job_prefix_override, N''),
@MAXDOP_scheduler_limit SMALLINT,
@MAXDOP_RG_limit_guess SMALLINT,
@MAXDOP_standard_edition_limit SMALLINT,
@MAXDOP_global_default SMALLINT,
@MAXDOP_database_level_default SMALLINT,
@MAXDOP_calculated_at_database_level SMALLINT; 

SET NOCOUNT ON;

SET @code_database_name = DB_NAME(); -- all code objects are required to exist on the same database and schema
SET @code_schema_name = OBJECT_SCHEMA_NAME(@@PROCID);
SET @partition_priority_algorithm_name = ISNULL(@partition_priority_algorithm_name, N'DEFAULT');
SET @rebuild_algorithm_name = ISNULL(@rebuild_algorithm_name, N'DEFAULT');
SET @ignore_archive_compressed_partitions = ISNULL(@ignore_archive_compressed_partitions, 1);
SET @reorg_use_COMPRESS_ALL_ROWGROUPS_option = ISNULL(@reorg_use_COMPRESS_ALL_ROWGROUPS_option, 1);
SET @reorg_execute_twice = ISNULL(@reorg_execute_twice, 0);
SET @rebuild_ONLINE_option = ISNULL(@rebuild_ONLINE_option, 0);
SET @prioritization_only = ISNULL(@prioritization_only, 0);
SET @disable_CPU_rescheduling = ISNULL(@disable_CPU_rescheduling, 0);

-- set default algorithm for partition priority calculation
SET @SQL_expression_for_partition_priority_calculation_DEFAULT = N'CASE WHEN cci_part_row_count < 1000 THEN 0
WHEN cci_part_deleted_row_count + cci_part_closed_row_count + cci_part_open_row_count >= 100000
THEN 10000000000000 + CAST(100.0 * (cci_part_deleted_row_count + cci_part_closed_row_count + cci_part_open_row_count) / cci_part_row_count AS INT)
WHEN (alter_last_complete_time IS NULL OR DATEDIFF(DAY, alter_last_complete_time, SYSUTCDATETIME()) >= 30)
AND (alter_last_full_timeout_time_utc IS NULL OR DATEDIFF(DAY, alter_last_full_timeout_time_utc, SYSUTCDATETIME()) >= 30)
THEN 1000000000000 + 
CASE WHEN cci_part_compressed_rowgroup_count_under_525k_rows > 0
THEN 100000000000 + CAST(100.0 * cci_part_compressed_rowgroup_count_under_525k_rows / cci_part_rowgroup_count AS INT)
ELSE cci_part_total_bytes / 1000000
END
WHEN cci_part_deleted_row_count + cci_part_closed_row_count + cci_part_open_row_count >= 1000
THEN 10000000000 + CAST(100.0 * (cci_part_deleted_row_count + cci_part_closed_row_count + cci_part_open_row_count) / cci_part_row_count AS INT)
ELSE CAST(100.0 * cci_part_compressed_rowgroup_count_under_525k_rows / cci_part_rowgroup_count AS INT)
END';

-- set default algorithm for rebuild calculation
SET @SQL_expression_for_rebuild_calculation_DEFAULT = N'
CASE
	WHEN alter_last_attempt_status_flag = 2 AND alter_last_attempt_was_rebuild = 1
	THEN 0
	WHEN sql_part_approx_row_count >= 8000000 AND (1.0 * cci_part_deleted_row_count / cci_part_row_count) > (1.6 / maxdop_limit)
	THEN 1 ELSE 0
END';
SET @SQL_expression_for_rebuild_calculation_NEVER = N'0';
SET @SQL_expression_for_rebuild_calculation_ALWAYS = N'1';


-- set runtime expression values to be passed to the child procedures
SET @used_SQL_expression_for_partition_priority_calculation = CASE @partition_priority_algorithm_name
	WHEN N'DEFAULT' THEN @SQL_expression_for_partition_priority_calculation_DEFAULT
	WHEN N'CUSTOM_PRESET' THEN @SQL_expression_for_partition_priority_calculation_CUSTOM_PRESET
	WHEN N'CUSTOM_USE_PARAMETER_EXPRESSION' THEN @SQL_expression_for_partition_priority_calculation
	ELSE NULL
END;

SET @used_SQL_expression_for_rebuild_calculation = CASE @rebuild_algorithm_name
	WHEN N'DEFAULT' THEN @SQL_expression_for_rebuild_calculation_DEFAULT
	WHEN N'CUSTOM_PRESET' THEN @SQL_expression_for_rebuild_calculation_CUSTOM_PRESET
	WHEN N'CUSTOM_USE_PARAMETER_EXPRESSION' THEN @SQL_expression_for_rebuild_calculation
	WHEN N'NEVER' THEN  @SQL_expression_for_rebuild_calculation_NEVER
	WHEN N'ALWAYS' THEN @SQL_expression_for_rebuild_calculation_ALWAYS
	ELSE NULL
END;


-- use stored procedure name and database for tables if optional logging parameters aren't set
SET @logging_database_name = ISNULL(@logging_database_name, DB_NAME());
SET @logging_schema_name = ISNULL(@logging_schema_name, OBJECT_SCHEMA_NAME(@@PROCID));


SET @is_valid_initial = 1;
EXEC [dbo].AgentJobMultiThread_InitialValidation
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@cleanup_stored_procedure_name = @cleanup_stored_procedure_name,
	@max_minutes_to_run = @max_minutes_to_run,
	@total_jobs_to_create = @max_CCI_alter_job_count,
	@is_valid_OUT = @is_valid_initial OUTPUT,
	@error_message_OUT = @nice_error_message OUTPUT;
	
IF @is_valid_initial = 0
BEGIN
	THROW 50000, @nice_error_message, 1;
	RETURN;
END;


-- fail if on older version than 2017 RTM or 2016 SP2
-- this is currently redundant but might matter if AgentJobMultiThread_InitialValidation is updated to work with SQL Server 2014
SET @product_version = TRY_CAST(PARSENAME(CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')), 4) AS INT);

IF @product_version < 13 OR (@product_version = 13 AND TRY_CAST(PARSENAME(CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')), 2) AS INT) < 5026)
BEGIN
	THROW 50140, 'Not tested on versions older than SQL Server 2016 SP2 and SQL Server 2017 RTM. Comment this code out at your own risk.', 1; 
	RETURN;
END;


-- ONLINE rebuild for CCI not supported until SQL Server 2019
IF @rebuild_ONLINE_option = 1 AND @product_version < 15
BEGIN
	THROW 50005, 'ONLINE rebuild for CCI not supported until SQL Server 2019. Change @rebuild_ONLINE_option to 0.', 1; 
	RETURN;
END;


IF NOT EXISTS (
	SELECT [compatibility_level]
	FROM sys.databases
	WHERE [name] = @code_database_name
	AND [compatibility_level] >= 130
)
BEGIN
	SET @nice_error_message = N'Compatibility level of at least 130 is required for the ' + QUOTENAME(@code_database_name) + N' database.';
	THROW 50145, @nice_error_message, 1; 
	RETURN;
END;


-- all necessary tables were created in the logging database and schema
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @dynamic_sql_result_set_exists_OUT = 1
FROM ' + QUOTENAME(@logging_database_name) + N'.sys.tables t
INNER JOIN ' + QUOTENAME(@logging_database_name) + N'.sys.schemas s ON t.[schema_id] = s.[schema_id]
where t.name IN (N''CCI_Reorg_Rebuild_Summary'', N''CCI_Reorg_Rebuild_Index_History'')
AND s.name = @logging_schema_name
HAVING COUNT_BIG(*) = 2';

SET @dynamic_sql_result_set_exists = 0;
EXEC sp_executesql @dynamic_sql_max,
N'@logging_schema_name SYSNAME, @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
@logging_schema_name = @logging_schema_name,
@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

IF @dynamic_sql_result_set_exists = 0
BEGIN
	THROW 50030, 'Cannot find required tables in logging database and schema. Check parameter values for @logging_database_name and @logging_schema_name or run the setup script again.', 1; 
	RETURN;
END;

-- all necessary procedures exist in the current database and schema
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @dynamic_sql_result_set_exists_OUT = 1
FROM ' + QUOTENAME(@code_database_name) + N'.sys.objects o
INNER JOIN ' + QUOTENAME(@code_database_name) + N'.sys.schemas s ON o.[schema_id] = s.[schema_id]
where o.name IN (N''CCI_Reorg_Rebuild_Child_Job'', N''CCI_Reorg_Rebuild_Cleanup_Jobs'',N''AgentJobMultiThread_InitialValidation'')
AND s.name = @code_schema_name
AND o.type = ''P''
HAVING COUNT_BIG(*) = 3';

SET @dynamic_sql_result_set_exists = 0;
EXEC sp_executesql @dynamic_sql_max,
N'@code_schema_name SYSNAME, @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
@code_schema_name = @code_schema_name,
@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

IF @dynamic_sql_result_set_exists = 0
BEGIN
	THROW 50035, 'Cannot find required stored procedures in logging database and schema. Check parameter values or run the setup script again.', 1; 
	RETURN;
END;

-- early validation of algorithm parameters
IF @Disable_SQL_Expression_Parameters = 1 AND @partition_priority_algorithm_name = N'CUSTOM_USE_PARAMETER_EXPRESSION'
BEGIN
	THROW 50061, 'CUSTOM_USE_PARAMETER_EXPRESSION option disabled by admin.', 1; 
	RETURN;
END;

IF @Disable_SQL_Expression_Parameters = 1 AND @rebuild_algorithm_name = N'CUSTOM_USE_PARAMETER_EXPRESSION'
BEGIN
	THROW 50071, 'CUSTOM_USE_PARAMETER_EXPRESSION option disabled by admin.', 1; 
	RETURN;
END;


-- this procedure cannot be called until the previous run has completed or should have completed
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT TOP (1) @dynamic_sql_result_set_exists_OUT = 1
FROM
(
	SELECT TOP (1) Summary_Start_Time_UTC, Max_Minutes_To_Run, Summary_End_Time_UTC
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
	ORDER BY Summary_Start_Time_UTC DESC
) q
WHERE q.Summary_End_Time_UTC IS NULL AND SYSUTCDATETIME() < DATEADD(MINUTE, 1 + Max_Minutes_To_Run, Summary_Start_Time_UTC)';

SET @dynamic_sql_result_set_exists = 0;
EXEC sp_executesql @dynamic_sql_max,
N'@dynamic_sql_result_set_exists_OUT BIT OUTPUT',
@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

IF @dynamic_sql_result_set_exists = 1
BEGIN
	SET @nice_error_message = N'Cannot run CCIReorgAndRebuild if previous run has not completed. Wait for the cleanup procedure to complete.'
	 + N' To clean up after a failed run, examine the CCI_Reorg_Rebuild_Summary table and consider running a query similar to: 
	 " WITH CTE AS (SELECT TOP (1) * FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary ORDER BY Summary_Start_Time_UTC DESC) DELETE FROM CTE; "';

	THROW 50120, @nice_error_message, 1; 
	RETURN;
END;


-- changing table or column names after release can break compatibility so don't do that ever
-- also can't create columns that contain "GO"
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'DROP TABLE IF EXISTS ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process';

EXEC sp_executesql @dynamic_sql_max;

-- use a string for the computed column instead of binary because conversion rules to binary can change between releases.
-- also non-negative NUMERIC to BINARY currently doesn't preserve order
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE TABLE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process (
	Database_Name SYSNAME NOT NULL,
	Schema_Name SYSNAME NOT NULL,
	Table_Name SYSNAME NOT NULL,
	Index_Name SYSNAME NOT NULL,
	Partition_Number INT NOT NULL,
	Database_Id INT NOT NULL,
	Object_Id INT NOT NULL,
	maxdop_limit SMALLINT NULL,
	partition_is_archive_compressed BIT NULL,
	alter_last_attempt_time_utc DATETIME2 NULL,
	alter_last_attempt_status_flag TINYINT NULL,
	alter_last_attempt_was_rebuild BIT NULL,
	alter_last_complete_time DATETIME2 NULL,
	alter_last_partial_timeout_time_utc DATETIME2 NULL,
	alter_last_full_timeout_time_utc DATETIME2 NULL,
	alter_last_error_time DATETIME2 NULL,
	sql_part_approx_row_count BIGINT DEFAULT 0,
	sql_part_approx_bytes BIGINT DEFAULT 0,
	sql_table_approx_row_count BIGINT DEFAULT 0,
	sql_table_approx_bytes BIGINT DEFAULT 0,
	sql_part_distance_from_rightmost_partition INT DEFAULT 0,
	cci_part_row_count BIGINT DEFAULT 0,
	cci_part_open_row_count BIGINT DEFAULT 0,
	cci_part_compressed_row_count BIGINT DEFAULT 0,
	cci_part_deleted_row_count BIGINT DEFAULT 0,
	cci_part_closed_row_count BIGINT DEFAULT 0,
	cci_part_tombstone_row_count BIGINT DEFAULT 0,
	cci_part_dict_pressure_row_count BIGINT DEFAULT 0,
	cci_part_memory_pressure_row_count BIGINT DEFAULT 0,
	cci_part_rowgroup_count BIGINT DEFAULT 0,
	cci_part_open_rowgroup_count BIGINT DEFAULT 0,
	cci_part_compressed_rowgroup_count BIGINT DEFAULT 0,
	cci_part_closed_rowgroup_count BIGINT DEFAULT 0,
	cci_part_tombstone_rowgroup_count BIGINT DEFAULT 0,
	cci_part_compressed_rowgroup_count_under_17k_rows BIGINT DEFAULT 0,
	cci_part_compressed_rowgroup_count_under_132k_rows BIGINT DEFAULT 0,
	cci_part_compressed_rowgroup_count_under_263k_rows BIGINT DEFAULT 0,
	cci_part_compressed_rowgroup_count_under_525k_rows BIGINT DEFAULT 0,
	cci_part_dict_pressure_rowgroup_count BIGINT DEFAULT 0,
	cci_part_memory_pressure_rowgroup_count BIGINT DEFAULT 0,
	cci_part_approx_deleted_rows_bytes BIGINT DEFAULT 0,
	cci_part_approx_compressed_bytes BIGINT DEFAULT 0,
	cci_part_approx_uncompressed_bytes BIGINT DEFAULT 0,
	cci_part_total_bytes BIGINT DEFAULT 0,
	cci_table_total_bytes BIGINT DEFAULT 0,
	cci_table_row_count BIGINT DEFAULT 0,	
	cci_table_open_row_count BIGINT DEFAULT 0,
	cci_table_compressed_row_count BIGINT DEFAULT 0,
	cci_table_deleted_row_count BIGINT DEFAULT 0,
	cci_table_closed_row_count BIGINT DEFAULT 0,
	cci_table_tombstone_row_count BIGINT DEFAULT 0,
	Calculated_Do_REBUILD BIT NOT NULL DEFAULT 0,
	Calculated_Priority NUMERIC(38, 0) NOT NULL DEFAULT 0,
	In_Progress SMALLINT NOT NULL,
	Prioritization_Complete_Time_UTC DATETIME2 NULL,
	Job_Number_That_Calculated_Priority SMALLINT NULL,
	Job_Number_That_Attempted_Alter SMALLINT NULL,
	String_Priority_Helper AS RIGHT(REPLICATE(''0'', 38) + CAST(Calculated_Priority AS VARCHAR(38)), 38) +
	RIGHT(''0000000000'' + CAST(Database_Id AS VARCHAR(10)), 10) + RIGHT(''0000000000'' + CAST(Object_Id AS VARCHAR(10)), 10),
	CONSTRAINT [CHK_NO_NEGATIVE_PRIORITY_' + SUBSTRING(CAST(RAND() AS NVARCHAR(10)), 3, 9) + N'] CHECK (Calculated_Priority >= 0)
) WITH (DATA_COMPRESSION = ROW)'

EXEC sp_executesql @dynamic_sql_max;


-- validate and process @CCI_included_database_name_list parameter
SELECT @database_count = COUNT_BIG([value])
FROM STRING_SPLIT(@CCI_included_database_name_list, @delimiter);

DECLARE @CCI_Database_Names TABLE ([database_id] INT NOT NULL, [database_name] SYSNAME NOT NULL);

INSERT INTO @CCI_Database_Names ([database_id], [database_name])
SELECT d.database_id, d.name
FROM STRING_SPLIT(@CCI_included_database_name_list, @delimiter) ss
INNER JOIN sys.databases d on ss.[value] = d.name;

SELECT @actual_database_count = @@ROWCOUNT;

IF @database_count <> @actual_database_count
BEGIN
	THROW 50040, 'At least one database name cannot be found. Note that database names containing commas are likely to cause issues. Consider using the @delimiter_override parameter.', 1; 
	RETURN;
END;

IF @actual_database_count = 0
BEGIN
	THROW 50041, 'Must pass in at least one database name in the @CCI_included_database_name_list parameter.', 1; 
	RETURN;
END;


-- check that user has necessary permissions and compat level on all databases
DECLARE CCI_Databases_Permission_Check CURSOR FOR   
SELECT [database_name]
FROM @CCI_Database_Names;  
  
OPEN CCI_Databases_Permission_Check;  
  
FETCH NEXT FROM CCI_Databases_Permission_Check INTO @current_CCI_database_name;
  
WHILE @@FETCH_STATUS = 0  
BEGIN
	IF HAS_PERMS_BY_NAME(@current_CCI_database_name, N'DATABASE', N'CREATE TABLE') = 0 -- this was the closest fit I could find
	BEGIN
		SET @nice_error_message = N'Permission to alter indexes (db_ddl_admin for example) is needed on ' + QUOTENAME(@current_CCI_database_name) + N' database.';
		THROW 50042, @nice_error_message, 1;
		RETURN;
	END;

	-- code throws error with compat 100 so catch that here - not sure why anyone would have CCIs in such a database though
	IF NOT EXISTS (
		SELECT [compatibility_level]
		FROM sys.databases
		WHERE [name] = @current_CCI_database_name
		AND [compatibility_level] >= 110
	)
	BEGIN
		SET @nice_error_message = N'Compat level cannot be level 100 in database ' + QUOTENAME(@current_CCI_database_name) + N'.';
		THROW 50045, @nice_error_message, 1;
		RETURN;
	END;

	FETCH NEXT FROM CCI_Databases_Permission_Check INTO @current_CCI_database_name;
END;

CLOSE CCI_Databases_Permission_Check;

DEALLOCATE CCI_Databases_Permission_Check;


-- validate @CCI_Excluded_Schema_Names if only one CCI database was passed in
IF @database_count = 1 AND @CCI_excluded_schema_name_list IS NOT NULL
BEGIN
	SET @current_CCI_database_name = REPLACE(@CCI_included_database_name_list, @delimiter, N'');

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT TOP (1) @dynamic_sql_result_set_exists_OUT = 1
	FROM STRING_SPLIT(@CCI_excluded_schema_name_list, @delimiter) split
	WHERE NOT EXISTS (
		SELECT 1
		FROM ' + QUOTENAME(@current_CCI_database_name) + N'.sys.schemas ss
		WHERE split.[value] = ss.[name]
	)';

	SET @dynamic_sql_result_set_exists = 0;
	EXEC sp_executesql @dynamic_sql_max,
	N'@CCI_excluded_schema_name_list NVARCHAR(4000), @delimiter NVARCHAR(1), @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
	@CCI_excluded_schema_name_list = @CCI_excluded_schema_name_list,
	@delimiter = @delimiter,
	@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

	IF @dynamic_sql_result_set_exists = 1
	BEGIN
		THROW 50043, 'Cannot find at least one schema in the @CCI_excluded_schema_name_list parameter.', 1; 
		RETURN;
	END;
END;


-- validate @CCI_excluded_table_name_list if only one CCI database was passed in
IF @database_count = 1 AND @CCI_excluded_table_name_list IS NOT NULL
BEGIN
	SET @current_CCI_database_name = REPLACE(@CCI_included_database_name_list, @delimiter, N'');

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT TOP (1) @dynamic_sql_result_set_exists_OUT = 1
	FROM STRING_SPLIT(@CCI_excluded_table_name_list, @delimiter) split
	WHERE NOT EXISTS (
		SELECT 1
		FROM ' + QUOTENAME(@current_CCI_database_name) + N'.sys.tables t
		WHERE split.[value] = t.[name]
	)';

	SET @dynamic_sql_result_set_exists = 0;
	EXEC sp_executesql @dynamic_sql_max,
	N'@CCI_excluded_table_name_list NVARCHAR(4000), @delimiter NVARCHAR(1), @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
	@CCI_excluded_table_name_list = @CCI_excluded_table_name_list,
	@delimiter = @delimiter,
	@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

	IF @dynamic_sql_result_set_exists = 1
	BEGIN
		THROW 50046, 'Cannot find at least one table in the @CCI_excluded_table_name_list parameter.', 1; 
		RETURN;
	END;
END;


-- process @CCI_Excluded_Schema_Names
CREATE TABLE #CCI_Excluded_Schema_Names ([schema_name] SYSNAME NOT NULL);

BEGIN TRY
	INSERT INTO #CCI_Excluded_Schema_Names ([schema_name])
	SELECT [value]
	FROM STRING_SPLIT(@CCI_excluded_schema_name_list, @delimiter);
END TRY
BEGIN CATCH
	-- most likely error 8152 or 2628 but no reason to catch that
	SET @nice_error_message = N'Error when processing @CCI_excluded_schema_name_list.'
	+ N' Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10))
	+ N' Error message: ' + LEFT(ERROR_MESSAGE(), 3600);

	THROW 50044, @nice_error_message, 1; 
END CATCH;

-- process @CCI_excluded_table_name_list
CREATE TABLE #CCI_Excluded_Table_Names ([table_name] SYSNAME NOT NULL);

BEGIN TRY
	INSERT INTO #CCI_Excluded_Table_Names ([table_name])
	SELECT [value]
	FROM STRING_SPLIT(@CCI_excluded_table_name_list, @delimiter);
END TRY
BEGIN CATCH
	-- most likely error 8152 or 2628 but no reason to catch that
	SET @nice_error_message = N'Error when processing @CCI_excluded_table_name_list.'
	+ N' Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10))
	+ N' Error message: ' + LEFT(ERROR_MESSAGE(), 3600);

	THROW 50047, @nice_error_message, 1; 
END CATCH;


-- validate numeric parameters
IF @rebuild_MAXDOP <= 0 OR @max_CCI_alter_job_count IS NULL OR @max_CCI_alter_job_count <= 0
BEGIN
	THROW 50050, 'Obvious error with @rebuild_MAXDOP and/or @max_CCI_alter_job_count parameter.', 1; 
	RETURN;
END;


-- validate priority parameters
IF @partition_priority_algorithm_name NOT IN (N'DEFAULT', N'CUSTOM_PRESET', N'CUSTOM_USE_PARAMETER_EXPRESSION')
BEGIN
	THROW 50060, 'Unimplemented value for @partition_priority_algorithm_name parameter. Check the documentation.', 1; 
	RETURN;
END;

IF @partition_priority_algorithm_name = N'CUSTOM_PRESET' AND @SQL_expression_for_partition_priority_calculation_CUSTOM_PRESET IS NULL
BEGIN
	THROW 50062, 'An admin must set the @SQL_expression_for_partition_priority_calculation_CUSTOM_PRESET local variable to enable the CUSTOM_PRESET option.', 1; 
	RETURN;
END;

IF @partition_priority_algorithm_name = N'CUSTOM_USE_PARAMETER_EXPRESSION' AND @SQL_expression_for_partition_priority_calculation IS NULL
BEGIN
	THROW 50064, 'Value must be set for @SQL_expression_for_partition_priority_calculation when CUSTOM_USE_PARAMETER_EXPRESSION option is selected.', 1; 
	RETURN;
END;

IF @partition_priority_algorithm_name <> N'CUSTOM_USE_PARAMETER_EXPRESSION' AND @SQL_expression_for_partition_priority_calculation IS NOT NULL
BEGIN
	THROW 50066, 'Setting a value for @SQL_expression_for_partition_priority_calculation is not supported because the CUSTOM_USE_PARAMETER_EXPRESSION option is not selected.', 1; 
	RETURN;
END;

IF @rebuild_algorithm_name NOT IN (N'DEFAULT', N'CUSTOM_PRESET', N'CUSTOM_USE_PARAMETER_EXPRESSION', N'NEVER', N'ALWAYS')
BEGIN
	THROW 50070, 'Unimplemented value for @rebuild_algorithm_name parameter. Check the documentation.', 1; 
	RETURN;
END;

IF @rebuild_algorithm_name = N'CUSTOM_PRESET' AND @SQL_expression_for_rebuild_calculation_CUSTOM_PRESET IS NULL
BEGIN
	THROW 50072, 'An admin must set the @SQL_expression_for_rebuild_calculation_CUSTOM_PRESET local variable to enable the CUSTOM_PRESET option.', 1; 
	RETURN;
END;

IF @rebuild_algorithm_name = N'CUSTOM_USE_PARAMETER_EXPRESSION' AND @SQL_expression_for_rebuild_calculation IS NULL
BEGIN
	THROW 50074, 'Setting a value for @SQL_expression_for_rebuild_calculation is not supported because the CUSTOM_USE_PARAMETER_EXPRESSION option is not selected.', 1; 
	RETURN;
END;

IF @rebuild_algorithm_name <> N'CUSTOM_USE_PARAMETER_EXPRESSION' AND @SQL_expression_for_rebuild_calculation IS NOT NULL
BEGIN
	THROW 50076, 'Value set for @SQL_expression_for_rebuild_calculation will be ignored because CUSTOM_USE_PARAMETER_EXPRESSION option is not selected.', 1; 
	RETURN;
END;


-- check that @SQL_expression_for_partition_priority_calculation and @used_SQL_expression_for_rebuild_calculation compile
CREATE TABLE #expression_dependent_columns (column_name SYSNAME NOT NULL);

IF @used_SQL_expression_for_partition_priority_calculation IS NOT NULL OR @used_SQL_expression_for_rebuild_calculation IS NOT NULL
BEGIN
	-- a weak defense
	IF CHARINDEX(N'GO', @SQL_expression_for_partition_priority_calculation) > 0 OR CHARINDEX(N'GO', @SQL_expression_for_rebuild_calculation) > 0
	BEGIN
		THROW 50010, N'"GO" cannot be used in @SQL_expression_for_partition_priority_calculation or @SQL_expression_for_rebuild_calculation.', 1; 
		RETURN;
	END;

	SET @view_name_with_schema = QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Get_Dependencies';

	SET @view_text = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE OR ALTER VIEW ' + @view_name_with_schema + N' AS
	SELECT CAST(' + ISNULL(@used_SQL_expression_for_partition_priority_calculation, N'0') + N' AS NUMERIC(38, 0)) COL1
	, CAST(' + ISNULL(@used_SQL_expression_for_rebuild_calculation, N'0') + N' AS BIT) COL2
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process';

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'USE ' + QUOTENAME(@logging_database_name) + N'; EXEC sp_executesql @view_text';

	-- create the view
	BEGIN TRY
		EXEC sp_executesql @dynamic_sql_max,
		N'@view_text NVARCHAR(MAX)',
		@view_text = @view_text;  
	END TRY
	BEGIN CATCH
		SET @nice_error_message = N'Error when validating @SQL_expression_for_partition_priority_calculation and @SQL_expression_for_rebuild_calculation.'
		+ N' Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10))
		+ N' Error message: ' + LEFT(ERROR_MESSAGE(), 3600);

		THROW 50082, @nice_error_message, 1; 
		RETURN;
	END CATCH;

	-- check for referenced tables other than CCI_Reorg_Rebuild_Partitions_To_Process
	SET @dynamic_sql_max  = N'SELECT TOP (1) @dynamic_sql_result_set_exists_OUT = 1
	FROM ' + QUOTENAME(@logging_database_name) + N'.sys.dm_sql_referenced_entities (@view_name_with_schema, ''OBJECT'')
	WHERE referenced_entity_name <> N''CCI_Reorg_Rebuild_Partitions_To_Process''';

	SET @dynamic_sql_result_set_exists = 0;
	EXEC sp_executesql @dynamic_sql_max,
	N'@view_name_with_schema NVARCHAR(400), @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
	@view_name_with_schema = @view_name_with_schema,
	@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

	IF @dynamic_sql_result_set_exists = 1
	BEGIN
		THROW 50084, 'Cannot reference tables other than CCI_Reorg_Rebuild_Partitions_To_Process in @SQL_expression_for_partition_priority_calculation and @SQL_expression_for_rebuild_calculation.', 1; 
		RETURN;
	END;

	-- check for compile errors. unfortunately can't catch some types of runtime errors like dividing by 0
	SET @dynamic_sql_max  = N'SELECT @dynamic_sql_result_set_exists_OUT = CAST(COUNT_BIG(COL1) + COUNT_BIG(COL2) AS BIT)
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + @view_name_with_schema;

	BEGIN TRY
		EXEC sp_executesql @dynamic_sql_max,
		N'@dynamic_sql_result_set_exists_OUT BIT OUTPUT',
		@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  
	END TRY
	BEGIN CATCH
		SET @nice_error_message = N'Error when validating @SQL_expression_for_partition_priority_calculation and @SQL_expression_for_rebuild_calculation. '
		+ N'@SQL_expression_for_partition_priority_calculation must cast to a NUMERIC(38,0) and @SQL_expression_for_rebuild_calculation must cast to a BIT.'
		+ N' Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10))
		+ N' Error message: ' + LEFT(ERROR_MESSAGE(), 3600);

		THROW 50086, @nice_error_message, 1; 
		RETURN;
	END CATCH;

	-- get column list
	SET @dynamic_sql_max  = N'INSERT INTO #expression_dependent_columns (column_name)
	SELECT DISTINCT referenced_minor_name
	FROM ' + QUOTENAME(@logging_database_name) + N'.sys.dm_sql_referenced_entities (@view_name_with_schema, ''OBJECT'')
	WHERE referenced_entity_name = N''CCI_Reorg_Rebuild_Partitions_To_Process''
	AND referenced_minor_name IS NOT NULL';

	EXEC sp_executesql @dynamic_sql_max,
	N'@view_name_with_schema NVARCHAR(400)',
	@view_name_with_schema = @view_name_with_schema;  


	-- check for banned columns (lower case columns are always allowed)
	IF EXISTS (SELECT 1
	FROM #expression_dependent_columns
	WHERE column_name IN (N'Calculated_Do_REBUILD', N'Calculated_Priority', N'In_Progress', N'Binary_Priority_Helper', N'Prioritization_Complete_Time_UTC')
	)
	BEGIN
		THROW 50088, N'@SQL_expression_for_partition_priority_calculation and @SQL_expression_for_rebuild_calculation cannot reference Calculated_Do_REBUILD, Calculated_Priority, In_Progress, Binary_Priority_Helper, or Prioritization_Complete_Time_UTC columns.', 1; 
		RETURN;
	END;

	-- determine which DMVs and calculations need to happen later based on used columns in the expressions
	SELECT
	  @query_part_level_info = MAX(CASE WHEN column_name LIKE N'sql[_]%' THEN 1 ELSE 0 END) 
	, @query_CCI_DMV_info = MAX(CASE WHEN column_name LIKE N'cci[_]%' THEN 1 ELSE 0 END) 
	, @query_history_table = MAX(CASE WHEN column_name LIKE N'alter[_]last[_]%' THEN 1 ELSE 0 END) 
	FROM #expression_dependent_columns;

	-- drop the view
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'USE ' + QUOTENAME(@logging_database_name) + N'; DROP VIEW IF EXISTS ' + @view_name_with_schema;

	EXEC sp_executesql @dynamic_sql_max;
END;


-- validate @start_stored_procedure_name_to_run
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @dynamic_sql_result_set_exists_OUT = 1
FROM ' + QUOTENAME(@code_database_name) + N'.sys.objects o
INNER JOIN ' + QUOTENAME(@code_database_name) + N'.sys.schemas s ON o.[schema_id] = s.[schema_id]
where o.name = @stored_procedure_name
AND s.name = @code_schema_name
AND [type] = ''P''';

IF @start_stored_procedure_name_to_run IS NOT NULL
BEGIN
	SET @dynamic_sql_result_set_exists = 1;
	EXEC sp_executesql @dynamic_sql_max,
	N'@stored_procedure_name SYSNAME, @code_schema_name SYSNAME, @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
	@stored_procedure_name = @start_stored_procedure_name_to_run,
	@code_schema_name = @code_schema_name,
	@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

	IF @dynamic_sql_result_set_exists = 0
	BEGIN
		THROW 50090, 'Cannot find @start_stored_procedure_name_to_run stored procedure. Be sure the stored procedure exists in the logging database and logging schema.', 1; 
		RETURN;
	END;
END;


-- validate @end_stored_procedure_name_to_run
IF @end_stored_procedure_name_to_run IS NOT NULL
BEGIN
	SET @dynamic_sql_result_set_exists = 1;
	EXEC sp_executesql @dynamic_sql_max,
	N'@stored_procedure_name SYSNAME, @code_schema_name SYSNAME, @dynamic_sql_result_set_exists_OUT BIT OUTPUT',
	@stored_procedure_name = @end_stored_procedure_name_to_run,
	@code_schema_name = @code_schema_name,
	@dynamic_sql_result_set_exists_OUT = @dynamic_sql_result_set_exists OUTPUT;  

	IF @dynamic_sql_result_set_exists = 0
	BEGIN
		THROW 50100, 'Cannot find @end_stored_procedure_name_to_run stored procedure. Be sure the stored procedure exists in the logging database and logging schema.', 1; 
		RETURN;
	END;
END;


-- passed all error checks!
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
(
  Summary_Start_Time_UTC	
, CCI_Included_Database_Name_List
, CCI_Excluded_Schema_Name_List
, CCI_Excluded_Table_Name_List
, Max_CCI_Alter_Job_Count
, Max_Minutes_To_Run
, Partition_Priority_Algorithm_Name
, Used_SQL_Expression_For_Partition_Priority_Calculation
, Rebuild_Algorithm_Name
, Used_SQL_Expression_For_Rebuild_Calculation
, Ignore_Archive_Compressed_Partitions
, Reorg_Use_COMPRESS_ALL_ROWGROUPS_Option
, Reorg_Execute_Twice
, Rebuild_MAXDOP
, Rebuild_ONLINE_Option
, Start_Stored_Procedure_Name_To_Run
, End_Stored_Procedure_Name_To_Run
, Disable_CPU_Rescheduling
, Delimiter_Override
, Used_Job_Prefix
, Prioritization_Only
, query_CCI_DMV_info
)
VALUES
(
  @start_time	
, @CCI_included_database_name_list
, @CCI_excluded_schema_name_list
, @CCI_excluded_table_name_list
, @max_CCI_alter_job_count
, @max_minutes_to_run
, @partition_priority_algorithm_name
, @used_SQL_expression_for_partition_priority_calculation
, @rebuild_algorithm_name
, @used_SQL_expression_for_rebuild_calculation
, @ignore_archive_compressed_partitions
, @reorg_use_COMPRESS_ALL_ROWGROUPS_option
, @reorg_execute_twice
, @rebuild_MAXDOP
, @rebuild_ONLINE_option
, @start_stored_procedure_name_to_run
, @end_stored_procedure_name_to_run
, @disable_CPU_rescheduling
, @delimiter_override
, @job_prefix
, @prioritization_only
, @query_CCI_DMV_info
)';

SET @parm_definition = N'@start_time DATETIME2, @CCI_included_database_name_list NVARCHAR(4000), @CCI_excluded_schema_name_list NVARCHAR(4000), @CCI_excluded_table_name_list NVARCHAR(4000)'
+ N',@max_CCI_alter_job_count SMALLINT, @max_minutes_to_run SMALLINT, @partition_priority_algorithm_name NVARCHAR(100), @used_SQL_expression_for_partition_priority_calculation NVARCHAR(4000)'
+ N',@rebuild_algorithm_name NVARCHAR(100), @used_SQL_expression_for_rebuild_calculation NVARCHAR(4000), @ignore_archive_compressed_partitions BIT'
+ N',@reorg_use_COMPRESS_ALL_ROWGROUPS_option BIT, @reorg_execute_twice BIT, @rebuild_MAXDOP INT, @rebuild_ONLINE_option BIT,@start_stored_procedure_name_to_run SYSNAME'
+ N',@end_stored_procedure_name_to_run SYSNAME, @disable_CPU_rescheduling BIT, @delimiter_override NVARCHAR(1), @job_prefix NVARCHAR(20), @prioritization_only BIT, @query_CCI_DMV_info BIT';

EXEC sp_executesql @dynamic_sql_max,
@parm_definition,
@start_time = @parent_start_time,
@CCI_included_database_name_list = @CCI_included_database_name_list,
@CCI_excluded_schema_name_list = @CCI_excluded_schema_name_list,
@CCI_excluded_table_name_list = @CCI_excluded_table_name_list,
@max_CCI_alter_job_count = @max_CCI_alter_job_count,
@max_minutes_to_run = @max_minutes_to_run,
@partition_priority_algorithm_name = @partition_priority_algorithm_name,
@used_SQL_expression_for_partition_priority_calculation = @used_SQL_expression_for_partition_priority_calculation,
@rebuild_algorithm_name = @rebuild_algorithm_name,
@used_SQL_expression_for_rebuild_calculation = @used_SQL_expression_for_rebuild_calculation,
@ignore_archive_compressed_partitions = @ignore_archive_compressed_partitions,
@reorg_use_COMPRESS_ALL_ROWGROUPS_option = @reorg_use_COMPRESS_ALL_ROWGROUPS_option,
@reorg_execute_twice = @reorg_execute_twice,
@rebuild_MAXDOP = @rebuild_MAXDOP,
@rebuild_ONLINE_option = @rebuild_ONLINE_option,
@start_stored_procedure_name_to_run = @start_stored_procedure_name_to_run,
@end_stored_procedure_name_to_run = @end_stored_procedure_name_to_run,
@disable_CPU_rescheduling = @disable_CPU_rescheduling,
@delimiter_override = @delimiter_override,
@job_prefix = @job_prefix,
@prioritization_only = @prioritization_only,
@query_CCI_DMV_info = @query_CCI_DMV_info;

-- run custom start stored procedure if set
IF @start_stored_procedure_name_to_run IS NOT NULL
BEGIN
	BEGIN TRY
		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + QUOTENAME(@code_schema_name) + N'.' + QUOTENAME(@start_stored_procedure_name_to_run);
		EXEC sp_executesql @dynamic_sql_max;
	END TRY
	BEGIN CATCH
		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
		SET
		  Custom_Start_Procedure_Name_Error_Text = ERROR_MESSAGE()
		, Summary_End_Time_UTC = SYSUTCDATETIME()
		WHERE Summary_Start_Time_UTC = @start_time';

		EXEC sp_executesql @dynamic_sql_max,
		N'@start_time DATETIME2',
		@start_time = @parent_start_time;

		SET @nice_error_message = N'Error with custom start procedure.'
		+ N' Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10))
		+ N' Error message: ' + LEFT(ERROR_MESSAGE(), 3600);

		THROW 50110, @nice_error_message, 1; 	

		RETURN;
	END CATCH;	
END;


-- quit if the custom procedure uses up all available time
IF DATEADD(MINUTE, @max_minutes_to_run, @parent_start_time) <= SYSUTCDATETIME()
BEGIN
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
	SET Summary_End_Time_UTC = SYSUTCDATETIME()
	, Alter_Statements_Completed = 0
	, Alter_Statements_Halted = 0
	, Alter_Statements_Not_Started = 0
	, Approximate_Error_Count = 0
	, Alter_Total_CPU_MS = 0
	WHERE Summary_Start_Time_UTC = @start_time';

	EXEC sp_executesql @dynamic_sql_max,
	N'@start_time DATETIME2',
	@start_time = @parent_start_time;

	RETURN;
END;


-- do some calculations to get information for a guess at maxdop_limit. not using conditional code because this is nearly free to get
SELECT @MAXDOP_scheduler_limit = COUNT_BIG(*)
FROM sys.dm_os_schedulers
WHERE [status] = N'VISIBLE ONLINE';

-- not using effective_max_dop because I assume this DMV is empty on standard edition
SELECT @MAXDOP_RG_limit_guess = ISNULL(MAX(wg.max_dop), 0)
FROM sys.dm_exec_requests r
INNER JOIN sys.resource_governor_workload_groups wg ON r.group_id = wg.group_id
WHERE r.session_id = @@SPID;

-- this is untested. I don't have access to standard edition
SET @MAXDOP_standard_edition_limit = CASE WHEN TRY_CAST(SERVERPROPERTY('EditionID') AS BIGINT) = -1534726760 THEN 2 ELSE 0 END;

SELECT @MAXDOP_global_default = TRY_CAST(value_in_use AS SMALLINT)
FROM sys.configurations
WHERE [name] = N'max degree of parallelism';


-- create a temp table to hold results of sys.dm_db_partition_stats dmv to avoid a not helpful spool that can appear
CREATE TABLE #pstats (
	[object_id] INT NOT NULL,
	index_id INT NOT NULL,
	partition_number INT NOT NULL,
	sql_part_approx_row_count BIGINT NOT NULL,
	sql_part_approx_bytes BIGINT NOT NULL,
	sql_table_approx_row_count BIGINT NOT NULL,
	sql_table_approx_bytes BIGINT NOT NULL,
	sql_part_distance_from_rightmost_partition INT NOT NULL,
	PRIMARY KEY ([object_id], index_id, partition_number)
);

-- create a temp table to hold results of sys.partitions dmv
CREATE TABLE #partitioned_indexes (
	[object_id] INT NOT NULL,
	index_id INT NOT NULL,
	PRIMARY KEY ([object_id], index_id)
);


-- loop over all requested databases and populate CCI_Reorg_Rebuild_Partitions_To_Process work table
BEGIN TRANSACTION;

EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';

DECLARE CCI_Databases CURSOR FOR   
SELECT [database_id], [database_name]
FROM @CCI_Database_Names;  
  
OPEN CCI_Databases;  
  
FETCH NEXT FROM CCI_Databases INTO @current_CCI_database_id, @current_CCI_database_name;
  
WHILE @@FETCH_STATUS = 0  
BEGIN
	-- get maxdop set at the database level
	SET @MAXDOP_database_level_default = 0;
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'SELECT @MAXDOP_database_level_default_OUT = TRY_CAST([value] AS SMALLINT)
	FROM ' + QUOTENAME(@current_CCI_database_name) + N'.sys.database_scoped_configurations
	WHERE [name] = N''MAXDOP''';

	EXEC sp_executesql @dynamic_sql_max,
	N'@MAXDOP_database_level_default_OUT SMALLINT OUTPUT',
	@MAXDOP_database_level_default_OUT = @MAXDOP_database_level_default OUTPUT;

	-- best guess at maxdop limit (child job could be running under different workload group but it adjusts for that if needed)
	SELECT @MAXDOP_calculated_at_database_level = MIN(maxdop_value)
	FROM (
	VALUES
		(COALESCE(@rebuild_MAXDOP, NULLIF(@MAXDOP_database_level_default, 0), NULLIF(@MAXDOP_global_default, 0), CASE WHEN @MAXDOP_scheduler_limit > 64 THEN 64 ELSE @MAXDOP_scheduler_limit END)),
		(@MAXDOP_scheduler_limit),
		(@MAXDOP_RG_limit_guess),
		(@MAXDOP_standard_edition_limit)
	) v (maxdop_value)
	WHERE v.maxdop_value <> 0;

	-- no lock hints because the code needs to find every index
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'TRUNCATE TABLE #partitioned_indexes;
	
	INSERT INTO #partitioned_indexes
	SELECT DISTINCT [object_id], index_id
	FROM ' + QUOTENAME(@current_CCI_database_name) + N'.sys.partitions sp2
	WHERE sp2.partition_number > 1;
	
	TRUNCATE TABLE #pstats;

	INSERT INTO #pstats (
	object_id
	, index_id
	, partition_number
	, sql_part_approx_row_count
	, sql_part_approx_bytes
	, sql_table_approx_row_count
	, sql_table_approx_bytes
	, sql_part_distance_from_rightmost_partition
	)
	SELECT
	  object_id
	, index_id
	, partition_number
	, row_count sql_part_approx_row_count
	, 8192 * reserved_page_count sql_part_approx_bytes
	, SUM(row_count) OVER (PARTITION BY object_id, index_id) sql_table_approx_row_count
	, SUM(8192 * reserved_page_count) OVER (PARTITION BY object_id, index_id) sql_table_approx_bytes
	, ROW_NUMBER() OVER (PARTITION BY object_id, index_id ORDER BY partition_number DESC) sql_part_distance_from_rightmost_partition
	FROM ' + QUOTENAME(@current_CCI_database_name) + N'.sys.dm_db_partition_stats
	WHERE 1 = ' + CAST(@query_part_level_info AS NVARCHAR(1)) + ';

	INSERT INTO ' 
	+ QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process WITH (TABLOCK)
	([Database_Name]
	, [Schema_Name]
	, Table_Name
	, Index_Name
	, Partition_Number
	, [Database_Id]
	, [Object_Id]
	, maxdop_limit
	, partition_is_archive_compressed
	, alter_last_attempt_time_utc
	, alter_last_attempt_status_flag
	, alter_last_attempt_was_rebuild
	, alter_last_complete_time
	, alter_last_partial_timeout_time_utc
	, alter_last_full_timeout_time_utc
	, alter_last_error_time	
	, sql_part_approx_row_count
	, sql_part_approx_bytes
	, sql_table_approx_row_count
	, sql_table_approx_bytes
	, sql_part_distance_from_rightmost_partition
	, In_Progress)
	SELECT 
	  @current_CCI_database_name
	, ss.name
	, st.name
	, si.name
	, ca.partition_number_not_null
	, @current_CCI_database_id
	, st.[object_id]
	, @MAXDOP_calculated_at_database_level
	, CASE WHEN sp.data_compression = 4 THEN 1 ELSE 0 END partition_is_archive_compressed
	, last_attempt.alter_last_attempt_time_utc
	, last_attempt.alter_last_attempt_status_flag
	, last_attempt.alter_last_attempt_was_rebuild
	, alter_last_complete_time.alter_last_complete_time
	, alter_last_partial_timeout_time_utc.alter_last_partial_timeout_time_utc
	, alter_last_full_timeout_time_utc.alter_last_full_timeout_time_utc
	, alter_last_error_time.alter_last_error_time	
	, pstats.sql_part_approx_row_count
	, pstats.sql_part_approx_bytes
	, pstats.sql_table_approx_row_count
	, pstats.sql_table_approx_bytes
	, pstats.sql_part_distance_from_rightmost_partition
	, 0
	FROM ' + QUOTENAME(@current_CCI_database_name) + N'.sys.indexes si
	INNER JOIN ' + QUOTENAME(@current_CCI_database_name) + N'.sys.partitions sp ON si.[object_id] = sp.[object_id] AND si.index_id = sp.index_id
	INNER JOIN ' + QUOTENAME(@current_CCI_database_name) + N'.sys.tables st ON si.[object_id] = st.[object_id]
	INNER JOIN ' + QUOTENAME(@current_CCI_database_name) + N'.sys.schemas ss ON st.[schema_id] = ss.[schema_id]
	LEFT OUTER JOIN ' + QUOTENAME(@current_CCI_database_name) + N'.sys.filegroups f ON f.data_space_id = si.data_space_id
	LEFT OUTER JOIN ' + QUOTENAME(@current_CCI_database_name) + N'.sys.destination_data_spaces ds ON ds.partition_scheme_id = si.data_space_id AND ds.destination_id = sp.partition_number
	LEFT OUTER JOIN ' + QUOTENAME(@current_CCI_database_name) + N'.sys.filegroups f2 ON f2.data_space_id = ds.data_space_id
	LEFT OUTER JOIN #partitioned_indexes pi ON si.[object_id] = pi.[object_id] AND si.index_id = pi.index_id
	CROSS APPLY (
		SELECT CASE WHEN pi.[object_id] IS NULL THEN -1 ELSE sp.partition_number END
	) ca (partition_number_not_null)
	LEFT OUTER JOIN #pstats pstats WITH (FORCESEEK) ON 1 = ' + CAST(@query_part_level_info AS NVARCHAR(1)) + ' AND pstats.object_id = sp.object_id AND pstats.index_id = sp.index_id AND pstats.partition_number = sp.partition_number
	OUTER APPLY (
		SELECT TOP (1) Alter_Start_Time_UTC alter_last_attempt_time_utc
		, CASE
			WHEN Did_Complete = 1 THEN 1
			WHEN Did_Error = 1 THEN 4
			WHEN Did_Stop = 1 AND Was_First_Alter_Of_Run = 1 THEN 2
			WHEN Did_Stop = 1 AND Was_First_Alter_Of_Run = 0 THEN 3
			ELSE NULL
			END alter_last_attempt_status_flag
		, Was_Rebuild alter_last_attempt_was_rebuild
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History c WITH (FORCESEEK)
		WHERE 1 = ' + CAST(@query_history_table AS NVARCHAR(1))
		+ N' AND c.Was_First_Alter_Of_Run IN (0, 1)
		AND c.Did_Complete IN (0, 1)
		AND c.Did_Error IN (0, 1)
		AND c.Did_Stop IN (0, 1)
		AND c.Database_Name = @current_CCI_database_name
		AND c.[Schema_Name] = ss.name COLLATE DATABASE_DEFAULT
		AND c.Table_Name = st.name COLLATE DATABASE_DEFAULT
		AND c.Index_Name = si.name COLLATE DATABASE_DEFAULT
		AND c.Partition_Number = ca.partition_number_not_null
		ORDER BY Alter_Start_Time_UTC DESC
	) last_attempt

	OUTER APPLY (
		SELECT TOP (1) Alter_Complete_Time_UTC alter_last_complete_time
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History c WITH (FORCESEEK)
		WHERE 1 = ' + CAST(@query_history_table AS NVARCHAR(1))
		+ N' AND c.Was_First_Alter_Of_Run IN (0, 1)
		AND c.Did_Complete IN (1)
		AND c.Did_Error IN (0)
		AND c.Did_Stop IN (0)
		AND c.Database_Name = @current_CCI_database_name
		AND c.[Schema_Name] = ss.name COLLATE DATABASE_DEFAULT
		AND c.Table_Name = st.name COLLATE DATABASE_DEFAULT
		AND c.Index_Name = si.name COLLATE DATABASE_DEFAULT
		AND c.Partition_Number = ca.partition_number_not_null
		ORDER BY Alter_Start_Time_UTC DESC
	) alter_last_complete_time

	OUTER APPLY (
		SELECT TOP (1) Alter_Stop_Time_UTC alter_last_partial_timeout_time_utc
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History c WITH (FORCESEEK)
		WHERE 1 = ' + CAST(@query_history_table AS NVARCHAR(1))
		+ N' AND c.Was_First_Alter_Of_Run IN (0)
		AND c.Did_Complete IN (0)
		AND c.Did_Error IN (0)
		AND c.Did_Stop IN (1)
		AND c.Database_Name = @current_CCI_database_name
		AND c.[Schema_Name] = ss.name COLLATE DATABASE_DEFAULT
		AND c.Table_Name = st.name COLLATE DATABASE_DEFAULT
		AND c.Index_Name = si.name COLLATE DATABASE_DEFAULT
		AND c.Partition_Number = ca.partition_number_not_null
		ORDER BY Alter_Start_Time_UTC DESC
	) alter_last_partial_timeout_time_utc

	OUTER APPLY (
		SELECT TOP (1) Alter_Stop_Time_UTC alter_last_full_timeout_time_utc
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History c WITH (FORCESEEK)
		WHERE 1 = ' + CAST(@query_history_table AS NVARCHAR(1))
		+ N' AND c.Was_First_Alter_Of_Run IN (1)
		AND c.Did_Complete IN (0)
		AND c.Did_Error IN (0)
		AND c.Did_Stop IN (1)
		AND c.Database_Name = @current_CCI_database_name
		AND c.[Schema_Name] = ss.name COLLATE DATABASE_DEFAULT
		AND c.Table_Name = st.name COLLATE DATABASE_DEFAULT
		AND c.Index_Name = si.name COLLATE DATABASE_DEFAULT
		AND c.Partition_Number = ca.partition_number_not_null
		ORDER BY Alter_Start_Time_UTC DESC
	) alter_last_full_timeout_time_utc

	OUTER APPLY (
		SELECT TOP (1) Alter_Stop_Time_UTC alter_last_error_time
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History c WITH (FORCESEEK)
		WHERE 1 = ' + CAST(@query_history_table AS NVARCHAR(1))
		+ N' AND c.Was_First_Alter_Of_Run IN (0, 1)
		AND c.Did_Complete IN (0)
		AND c.Did_Error IN (1)
		AND c.Did_Stop IN (1)
		AND c.Database_Name = @current_CCI_database_name
		AND c.[Schema_Name] = ss.name COLLATE DATABASE_DEFAULT
		AND c.Table_Name = st.name COLLATE DATABASE_DEFAULT
		AND c.Index_Name = si.name COLLATE DATABASE_DEFAULT
		AND c.Partition_Number = ca.partition_number_not_null
		ORDER BY Alter_Start_Time_UTC DESC
	) alter_last_error_time

	where si.type  = 5
	AND ISNULL(f.is_read_only, f2.is_read_only) = 0 '
	+ CASE WHEN @ignore_archive_compressed_partitions = 1 THEN N'AND sp.data_compression = 3 ' ELSE N'' END
	+ N' AND NOT EXISTS (
		SELECT 1
		FROM #CCI_Excluded_Schema_Names fs
		WHERE fs.schema_name = ss.name COLLATE DATABASE_DEFAULT
	)
	AND NOT EXISTS (
		SELECT 1
		FROM #CCI_Excluded_Table_Names ft
		WHERE ft.table_name = st.name COLLATE DATABASE_DEFAULT
	)
	OPTION (NO_PERFORMANCE_SPOOL, USE HINT(''FORCE_DEFAULT_CARDINALITY_ESTIMATION''))';

	EXEC sp_executesql @dynamic_sql_max,
	N'@current_CCI_database_name SYSNAME, @current_CCI_database_id INT, @MAXDOP_calculated_at_database_level SMALLINT',
	@current_CCI_database_name = @current_CCI_database_name,
	@current_CCI_database_id = @current_CCI_database_id,
	@MAXDOP_calculated_at_database_level = @MAXDOP_calculated_at_database_level;

	FETCH NEXT FROM CCI_Databases INTO @current_CCI_database_id, @current_CCI_database_name;
END;

CLOSE CCI_Databases;

DEALLOCATE CCI_Databases;

-- create NCIs after all data has been inserted
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'CREATE INDEX [NCI_String_Priority_Helper_' + SUBSTRING(CAST(RAND() AS NVARCHAR(10)), 3, 9) + N']
ON ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process
(In_Progress, String_Priority_Helper, Partition_Number);

CREATE INDEX [NCI_Database_Object_' + SUBSTRING(CAST(RAND() AS NVARCHAR(10)), 3, 9) + N']
ON ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process
(Database_Id, Object_Id)';

EXEC sp_executesql @dynamic_sql_max;

EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';
		
COMMIT TRANSACTION;


-- create and populate a work table of distinct database_name, database_id, and object_id for child jobs to use as queue
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'DROP TABLE IF EXISTS ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Tables_To_Process;
CREATE TABLE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Tables_To_Process (
	Database_Name SYSNAME NOT NULL,	
	Database_Id INT NOT NULL,
	Object_Id INT NOT NULL,
	Single_Partition_Only BIT NOT NULL
)';

EXEC sp_executesql @dynamic_sql_max;

SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Tables_To_Process
WITH (TABLOCKX)
(Database_Name, Database_Id, Object_Id, Single_Partition_Only)
SELECT Database_Name, Database_Id, Object_Id, MAX(CASE WHEN Partition_Number = -1 THEN 1 ELSE 0 END) Single_Partition_Only
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process
GROUP BY Database_Name, Database_Id, Object_Id';

EXEC sp_executesql @dynamic_sql_max;


EXEC [dbo].AgentJobMultiThread_CreateAgentJobs
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@parent_start_time = @parent_start_time,
	@child_stored_procedure_name = @child_stored_procedure_name,
	@cleanup_stored_procedure_name = @cleanup_stored_procedure_name,
	@max_minutes_to_run = @max_minutes_to_run,
	@job_prefix = @job_prefix,
	@total_jobs_to_create = @max_CCI_alter_job_count;

END;

GO







CREATE OR ALTER PROCEDURE [dbo].[CCI_Reorg_Rebuild_Child_Job] (
	@logging_database_name SYSNAME,
	@logging_schema_name SYSNAME,
	@parent_start_time DATETIME2,
	@job_number SMALLINT,
	@job_attempt_number SMALLINT
)
AS
BEGIN
/*
Procedure Name: CCI_Reorg_Rebuild_Child_Job
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: To be called by the multithreading job framework. You should not be executing this stored procedure yourself.
*/
DECLARE @workload_identifier NVARCHAR(50) = N'CCI_Reorg_Rebuild',
@child_stored_procedure_name SYSNAME = N'CCI_Reorg_Rebuild_Child_Job',
@all_partitions_processed BIT = 0,
@all_tables_processed BIT = 0,
@database_name SYSNAME,
@database_id INT,
@schema_name SYSNAME,
@table_name SYSNAME,
@object_id INT,
@index_name SYSNAME,
@partition_number INT,
@string_priority_helper VARCHAR(58),
@calculated_priority NUMERIC(38, 0),
@calculated_do_REBUILD BIT,
@Single_Partition_Only BIT,
@reorg_execute_twice BIT,
@alter_sql NVARCHAR(4000),
@dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@dynamic_sql_result_set_exists BIT,
@parm_definition NVARCHAR(4000),
@first_alter BIT = 1,
@cpu_time_snapshot INT,
@cpu_time_delta INT,
@alter_start_time DATETIME2,
@error_message NVARCHAR(4000),
@reorg_use_COMPRESS_ALL_ROWGROUPS_option BIT,
@rebuild_MAXDOP INT,
@rebuild_ONLINE_option BIT,
@used_SQL_expression_for_partition_priority_calculation NVARCHAR(4000),
@used_SQL_expression_for_rebuild_calculation NVARCHAR(4000),
@was_job_rescheduled BIT,
@disable_CPU_rescheduling BIT,
@job_prefix NVARCHAR(20),
@prioritization_only BIT,
@query_CCI_DMV_info BIT,
@MAXDOP_RG_limit SMALLINT,
@should_job_stop BIT;

SET NOCOUNT ON;

-- get needed parameter values from the summary table for this run
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'SELECT
    @reorg_use_COMPRESS_ALL_ROWGROUPS_option_OUT = Reorg_Use_COMPRESS_ALL_ROWGROUPS_Option
  , @reorg_execute_twice_OUT = Reorg_Execute_Twice
  , @used_SQL_expression_for_partition_priority_calculation_OUT = Used_SQL_Expression_For_Partition_Priority_Calculation
  , @used_SQL_expression_for_rebuild_calculation_OUT = Used_SQL_Expression_For_Rebuild_Calculation
  , @rebuild_MAXDOP_OUT = Rebuild_MAXDOP
  , @rebuild_ONLINE_option_OUT = Rebuild_ONLINE_Option
  , @disable_CPU_rescheduling_OUT = Disable_CPU_Rescheduling
  , @used_job_prefix_OUT = Used_Job_Prefix 
  , @prioritization_only_OUT = Prioritization_Only
  , @query_CCI_DMV_info_OUT = query_CCI_DMV_info
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
WHERE Summary_Start_Time_UTC = @parent_start_time';

SET @parm_definition = N'@reorg_use_COMPRESS_ALL_ROWGROUPS_option_OUT BIT OUTPUT, @reorg_execute_twice_OUT BIT OUTPUT, @used_SQL_expression_for_partition_priority_calculation_OUT NVARCHAR(4000) OUTPUT'
+ N',@used_SQL_expression_for_rebuild_calculation_OUT NVARCHAR(4000) OUTPUT, @rebuild_MAXDOP_OUT INT OUTPUT, @rebuild_ONLINE_option_OUT BIT OUTPUT'
+ N', @disable_CPU_rescheduling_OUT BIT OUTPUT, @used_job_prefix_OUT NVARCHAR(100) OUTPUT, @prioritization_only_OUT BIT OUTPUT, @query_CCI_DMV_info_OUT BIT OUTPUT, @parent_start_time DATETIME2';

EXEC sp_executesql @dynamic_sql_max,
@parm_definition,
@reorg_use_COMPRESS_ALL_ROWGROUPS_option_OUT = @reorg_use_COMPRESS_ALL_ROWGROUPS_option OUTPUT,
@reorg_execute_twice_OUT = @reorg_execute_twice OUTPUT,
@used_SQL_expression_for_partition_priority_calculation_OUT = @used_SQL_expression_for_partition_priority_calculation OUTPUT,
@used_SQL_expression_for_rebuild_calculation_OUT = @used_SQL_expression_for_rebuild_calculation OUTPUT,
@rebuild_MAXDOP_OUT = @rebuild_MAXDOP OUTPUT,
@rebuild_ONLINE_option_OUT = @rebuild_ONLINE_option OUTPUT,
@disable_CPU_rescheduling_OUT = @disable_CPU_rescheduling OUTPUT,
@used_job_prefix_OUT = @job_prefix OUTPUT,
@prioritization_only_OUT = @prioritization_only OUTPUT,
@query_CCI_DMV_info_OUT = @query_CCI_DMV_info OUTPUT,
@parent_start_time = @parent_start_time;
   
-- there was likely a problem with the parent procedure if this is NULL
IF @reorg_use_COMPRESS_ALL_ROWGROUPS_option IS NULL
BEGIN
	THROW 60000, 'Cannot find expected row in CCI_Reorg_Rebuild_Summary table. Look for an error logged by the CCIReorgAndRebuild stored procedure.', 1; 
	RETURN;
END;


IF @disable_CPU_rescheduling = 0
BEGIN
	EXEC dbo.AgentJobMultiThread_RescheduleChildJobIfNeeded 
		@workload_identifier = @workload_identifier,
		@logging_database_name = @logging_database_name,
		@logging_schema_name = @logging_schema_name,
		@parent_start_time = @parent_start_time,
		@child_stored_procedure_name = @child_stored_procedure_name,
		@job_prefix = @job_prefix,
		@job_number = @job_number,
		@job_attempt_number = @job_attempt_number,
		@was_job_rescheduled_OUT = @was_job_rescheduled OUTPUT;

	IF @was_job_rescheduled = 1
	BEGIN
		RETURN;
	END;
END;


-- target table for sys.dm_db_column_store_row_group_physical_stats
CREATE TABLE #cci_dmv_results (
 	Partition_Number INT NOT NULL,
	cci_part_row_count BIGINT NULL,
	cci_part_open_row_count BIGINT NULL,
	cci_part_compressed_row_count BIGINT NULL,
	cci_part_deleted_row_count BIGINT NULL,
	cci_part_closed_row_count BIGINT NULL,
	cci_part_tombstone_row_count BIGINT NULL,
	cci_part_dict_pressure_row_count BIGINT NULL,
	cci_part_memory_pressure_row_count BIGINT NULL,
	cci_part_rowgroup_count BIGINT NULL,
	cci_part_open_rowgroup_count BIGINT NULL,
	cci_part_compressed_rowgroup_count BIGINT NULL,
	cci_part_closed_rowgroup_count BIGINT NULL,
	cci_part_tombstone_rowgroup_count BIGINT NULL,
	cci_part_compressed_rowgroup_count_under_17k_rows BIGINT NULL,
	cci_part_compressed_rowgroup_count_under_132k_rows BIGINT NULL,
	cci_part_compressed_rowgroup_count_under_263k_rows BIGINT NULL,
	cci_part_compressed_rowgroup_count_under_525k_rows BIGINT NULL,
	cci_part_dict_pressure_rowgroup_count BIGINT NULL,
	cci_part_memory_pressure_rowgroup_count BIGINT NULL,
	cci_part_approx_deleted_rows_bytes BIGINT NULL,
	cci_part_approx_compressed_bytes BIGINT NULL,
	cci_part_approx_uncompressed_bytes BIGINT NULL,
	cci_part_total_bytes BIGINT NULL,
	cci_table_total_bytes BIGINT NULL,
	cci_table_row_count BIGINT NULL,	
	cci_table_open_row_count BIGINT NULL,
	cci_table_compressed_row_count BIGINT NULL,
	cci_table_deleted_row_count BIGINT NULL,
	cci_table_closed_row_count BIGINT NULL,
	cci_table_tombstone_row_count BIGINT NULL
);


-- need to check this here again in case the parent procedure was running in a different workload group than this session
SELECT @MAXDOP_RG_limit = ISNULL(MAX(wg.max_dop), 0)
FROM sys.dm_exec_requests r
INNER JOIN sys.resource_governor_workload_groups wg ON r.group_id = wg.group_id
WHERE r.session_id = @@SPID;


SET @should_job_stop = 0;
-- loop through tables and set needed columns in CCI_Reorg_Rebuild_Partitions_To_Process
WHILE @all_tables_processed = 0
BEGIN
	SET @database_name = NULL;
	SET @database_id = NULL;
	SET @object_id = NULL;
	SET @Single_Partition_Only = NULL;

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'DECLARE @hold_deleted_row TABLE (
	Database_Name SYSNAME,
	Database_Id INT,
	Object_Id INT,
	Single_Partition_Only BIT
	);

	DELETE TOP (1) FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Tables_To_Process
	WITH (TABLOCKX)
	OUTPUT deleted.Database_Name, deleted.Database_Id, deleted.Object_Id, deleted.Single_Partition_Only INTO @hold_deleted_row;

	SELECT @database_name_OUT = Database_Name
	, @database_id_OUT = Database_Id
	, @object_id_OUT = Object_Id
	, @single_partition_only_OUT = Single_Partition_Only
	FROM @hold_deleted_row';

	EXEC sp_executesql @dynamic_sql_max,
	N'@database_name_OUT SYSNAME OUTPUT, @database_id_OUT INT OUTPUT, @object_id_OUT INT OUTPUT, @single_partition_only_OUT BIT OUTPUT',
	@database_name_OUT = @database_name OUTPUT,
	@database_id_OUT = @database_id OUTPUT,
	@object_id_OUT = @object_id OUTPUT,
	@single_partition_only_OUT = @Single_Partition_Only OUTPUT;

	-- if NULL then there are no more rows for this child job to process from the table
	IF @database_name IS NOT NULL
	BEGIN
		-- querying sys.dm_db_column_store_row_group_physical_stats can be expensive, so only query at the object level and only if it contains a needed column for the algorithms
		IF @query_CCI_DMV_info = 1
		BEGIN
			TRUNCATE TABLE #cci_dmv_results;

			SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'INSERT INTO #cci_dmv_results
			(
			  Partition_Number
			, cci_part_row_count
			, cci_part_open_row_count
			, cci_part_compressed_row_count
			, cci_part_deleted_row_count
			, cci_part_closed_row_count
			, cci_part_tombstone_row_count
			, cci_part_dict_pressure_row_count
			, cci_part_memory_pressure_row_count
			, cci_part_rowgroup_count
			, cci_part_open_rowgroup_count
			, cci_part_compressed_rowgroup_count
			, cci_part_closed_rowgroup_count
			, cci_part_tombstone_rowgroup_count
			, cci_part_compressed_rowgroup_count_under_17k_rows
			, cci_part_compressed_rowgroup_count_under_132k_rows
			, cci_part_compressed_rowgroup_count_under_263k_rows
			, cci_part_compressed_rowgroup_count_under_525k_rows
			, cci_part_dict_pressure_rowgroup_count
			, cci_part_memory_pressure_rowgroup_count
			, cci_part_approx_deleted_rows_bytes
			, cci_part_approx_compressed_bytes
			, cci_part_approx_uncompressed_bytes
			, cci_part_total_bytes
			, cci_table_total_bytes
			, cci_table_row_count
			, cci_table_open_row_count
			, cci_table_compressed_row_count
			, cci_table_deleted_row_count
			, cci_table_closed_row_count
			, cci_table_tombstone_row_count
			)
			SELECT q.*
			, SUM(cci_part_total_bytes) OVER () cci_table_total_bytes
			, SUM(cci_part_row_count) OVER () cci_table_row_count
			, SUM(cci_part_open_row_count) OVER () cci_table_open_row_count
			, SUM(cci_part_compressed_row_count) OVER () cci_table_compressed_row_count
			, SUM(cci_part_deleted_row_count) OVER () cci_table_deleted_row_count
			, SUM(cci_part_closed_row_count) OVER () cci_table_closed_row_count
			, SUM(cci_part_tombstone_row_count) OVER () cci_table_tombstone_row_count
			FROM
			(
				SELECT
				  ' + CASE WHEN @Single_Partition_Only = 1 THEN N'-1' ELSE N'' END + N' Partition_Number
				, SUM(total_rows) cci_part_row_count
				, SUM(CASE WHEN state = 1 THEN total_rows ELSE 0 END) cci_part_open_row_count
				, SUM(CASE WHEN state = 3 THEN total_rows ELSE 0 END) cci_part_compressed_row_count
				, SUM(deleted_rows) cci_part_deleted_row_count
				, SUM(CASE WHEN state = 2 THEN total_rows ELSE 0 END) cci_part_closed_row_count
				, SUM(CASE WHEN state = 4 THEN total_rows ELSE 0 END) cci_part_tombstone_row_count
				, SUM(CASE WHEN trim_reason = 4 THEN total_rows ELSE 0 END) cci_part_dict_pressure_row_count
				, SUM(CASE WHEN trim_reason = 5 THEN total_rows ELSE 0 END) cci_part_memory_pressure_row_count
				, COUNT_BIG(*) cci_part_rowgroup_count
				, SUM(CASE WHEN state = 1 THEN 1 ELSE 0 END) cci_part_open_rowgroup_count
				, SUM(CASE WHEN state = 3 THEN 1 ELSE 0 END) cci_part_compressed_rowgroup_count
				, SUM(CASE WHEN state = 2 THEN 1 ELSE 0 END) cci_part_closed_rowgroup_count
				, SUM(CASE WHEN state = 4 THEN 1 ELSE 0 END) cci_part_tombstone_rowgroup_count
				, SUM(CASE WHEN state = 3 AND total_rows < 17000 THEN 1 ELSE 0 END) cci_part_compressed_rowgroup_count_under_17k_rows
				, SUM(CASE WHEN state = 3 AND total_rows < 132000 THEN 1 ELSE 0 END) cci_part_compressed_rowgroup_count_under_132k_rows
				, SUM(CASE WHEN state = 3 AND total_rows < 263000 THEN 1 ELSE 0 END) cci_part_compressed_rowgroup_count_under_263k_rows
				, SUM(CASE WHEN state = 3 AND total_rows < 525000 THEN 1 ELSE 0 END) cci_part_compressed_rowgroup_count_under_525k_rows
				, SUM(CASE WHEN trim_reason = 4 THEN 1 ELSE 0 END) cci_part_dict_pressure_rowgroup_count
				, SUM(CASE WHEN trim_reason = 5 THEN 1 ELSE 0 END) cci_part_memory_pressure_rowgroup_count
				, SUM(CASE WHEN total_rows = 0 THEN 0 ELSE 1.0 * size_in_bytes * deleted_rows / total_rows END) cci_part_approx_deleted_rows_bytes
				, SUM(CASE WHEN total_rows = 0 OR state <> 3 THEN 0 ELSE 1.0 * size_in_bytes * (total_rows - deleted_rows) / total_rows END) cci_part_approx_compressed_bytes
				, SUM(CASE WHEN state IN (1, 2) THEN size_in_bytes ELSE 0 END) cci_part_approx_uncompressed_bytes
				, SUM(size_in_bytes) cci_part_total_bytes
				FROM ' + QUOTENAME(@database_name) + N'.sys.dm_db_column_store_row_group_physical_stats
				WHERE object_id = @object_id
				GROUP BY Partition_Number
			)  q';

			EXEC sp_executesql @dynamic_sql_max,
			N'@object_id INT',
			@object_id = @object_id;

		END;

		BEGIN TRANSACTION;
		EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';

		IF @query_CCI_DMV_info = 1
		BEGIN
			SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'UPDATE t SET
			  t.maxdop_limit = CASE WHEN @MAXDOP_RG_limit > 0 AND maxdop_limit > @MAXDOP_RG_limit THEN @MAXDOP_RG_limit ELSE t.maxdop_limit END
			, t.cci_part_row_count = s.cci_part_row_count
			, t.cci_part_open_row_count = s.cci_part_open_row_count
			, t.cci_part_compressed_row_count = s.cci_part_compressed_row_count
			, t.cci_part_deleted_row_count = s.cci_part_deleted_row_count
			, t.cci_part_closed_row_count = s.cci_part_closed_row_count
			, t.cci_part_tombstone_row_count = s.cci_part_tombstone_row_count
			, t.cci_part_dict_pressure_row_count = s.cci_part_dict_pressure_row_count
			, t.cci_part_memory_pressure_row_count = s.cci_part_memory_pressure_row_count
			, t.cci_part_rowgroup_count = s.cci_part_rowgroup_count
			, t.cci_part_open_rowgroup_count = s.cci_part_open_rowgroup_count
			, t.cci_part_compressed_rowgroup_count = s.cci_part_compressed_rowgroup_count
			, t.cci_part_closed_rowgroup_count = s.cci_part_closed_rowgroup_count
			, t.cci_part_tombstone_rowgroup_count = s.cci_part_tombstone_rowgroup_count
			, t.cci_part_compressed_rowgroup_count_under_17k_rows = s.cci_part_compressed_rowgroup_count_under_17k_rows
			, t.cci_part_compressed_rowgroup_count_under_132k_rows = s.cci_part_compressed_rowgroup_count_under_132k_rows
			, t.cci_part_compressed_rowgroup_count_under_263k_rows = s.cci_part_compressed_rowgroup_count_under_263k_rows
			, t.cci_part_compressed_rowgroup_count_under_525k_rows = s.cci_part_compressed_rowgroup_count_under_525k_rows
			, t.cci_part_dict_pressure_rowgroup_count = s.cci_part_dict_pressure_rowgroup_count
			, t.cci_part_memory_pressure_rowgroup_count = s.cci_part_memory_pressure_rowgroup_count
			, t.cci_part_approx_deleted_rows_bytes = s.cci_part_approx_deleted_rows_bytes
			, t.cci_part_approx_compressed_bytes = s.cci_part_approx_compressed_bytes
			, t.cci_part_approx_uncompressed_bytes = s.cci_part_approx_uncompressed_bytes
			, t.cci_part_total_bytes = s.cci_part_total_bytes
			, t.cci_table_total_bytes = s.cci_table_total_bytes
			, t.cci_table_row_count = s.cci_table_row_count
			, t.cci_table_open_row_count = s.cci_table_open_row_count
			, t.cci_table_compressed_row_count = s.cci_table_compressed_row_count
			, t.cci_table_deleted_row_count = s.cci_table_deleted_row_count
			, t.cci_table_closed_row_count = s.cci_table_closed_row_count
			, t.cci_table_tombstone_row_count = s.cci_table_tombstone_row_count
			FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process t
			INNER JOIN #cci_dmv_results s ON t.Partition_Number = s.Partition_Number
			WHERE t.Database_Id = @database_id AND t.Object_Id = @object_id';

			EXEC sp_executesql @dynamic_sql_max,
			N'@database_id INT, @object_id INT, @MAXDOP_RG_limit SMALLINT',
			@database_id = @database_id,
			@object_id = @object_id,
			@MAXDOP_RG_limit = @MAXDOP_RG_limit;
		END
		ELSE IF @MAXDOP_RG_limit <> 0
		BEGIN
			SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'UPDATE t SET t.maxdop_limit = @MAXDOP_RG_limit
			FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process t
			WHERE t.Database_Id = @database_id AND t.Object_Id = @object_id
			AND maxdop_limit > @MAXDOP_RG_limit';

			EXEC sp_executesql @dynamic_sql_max,
			N'@database_id INT, @object_id INT, @MAXDOP_RG_limit SMALLINT',
			@database_id = @database_id,
			@object_id = @object_id,
			@MAXDOP_RG_limit = @MAXDOP_RG_limit;
		END;

		-- calculate priority and rebuild vs reorg after all other columns are set
		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process
			SET
			    Calculated_Priority = ' + @used_SQL_expression_for_partition_priority_calculation + N'
			  , Job_Number_That_Calculated_Priority = @job_number
			  , Prioritization_Complete_Time_UTC = SYSUTCDATETIME()
			  , In_Progress = CASE WHEN ' + @used_SQL_expression_for_partition_priority_calculation + ' = 0 THEN -1 ELSE In_Progress END
			  , Calculated_Do_REBUILD = ' + @used_SQL_expression_for_rebuild_calculation + 
		    N' WHERE Database_Id = @database_id AND Object_Id = @object_id';

		EXEC sp_executesql @dynamic_sql_max,
		N'@job_number SMALLINT, @database_id INT, @object_id INT',
		@job_number = @job_number,
		@database_id = @database_id,
		@object_id = @object_id;

	  	EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';	
		COMMIT TRANSACTION;


		EXEC [dbo].AgentJobMultiThread_ShouldChildJobHalt 
			@workload_identifier = @workload_identifier,
			@logging_database_name = @logging_database_name,
			@logging_schema_name = @logging_schema_name,
			@parent_start_time = @parent_start_time,
			@should_job_halt_OUT = @should_job_stop OUTPUT;

		IF @should_job_stop = 1
		BEGIN
			RETURN;
		END;
	END
	ELSE
	BEGIN -- @database_name can only be null if there are no more rows for this job to process
		SET @all_tables_processed = 1;
	END;

END;

-- don't do any maintenance operations if end user just wants prioritization
IF @prioritization_only = 1
BEGIN
	RETURN;
END;


-- loop through all partitions that need a maintenance action
WHILE @all_partitions_processed = 0
BEGIN
	SET @database_name = NULL;
	SET @schema_name = NULL;
	SET @table_name = NULL;
	SET @index_name = NULL;
	SET @partition_number = NULL;
	SET @string_priority_helper = NULL;
	SET @calculated_do_REBUILD = NULL;
	SET @calculated_priority = NULL;

	BEGIN TRANSACTION;
	EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';

	-- this is the easy way to write it, believe it or not
	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) + N'WITH ConsultantBux
	AS
	(
		SELECT TOP (1) 
		  wt.[Database_Name]
		, wt.[Schema_Name]
		, wt.Table_Name
		, wt.Index_Name
		, wt.Partition_Number
		, wt.String_Priority_Helper
		, wt.Calculated_Do_REBUILD
		, wt.Calculated_Priority
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process wt
		WHERE wt.In_Progress = 0
		AND wt.Calculated_Priority > 0
		ORDER BY wt.String_Priority_Helper DESC, wt.Partition_Number DESC
 
		UNION ALL
 
		SELECT 
		  R.[Database_Name]
		, R.[Schema_Name]
		, R.Table_Name
		, R.Index_Name
		, R.Partition_Number
		, R.String_Priority_Helper
		, R.Calculated_Do_REBUILD
		, R.Calculated_Priority
		FROM
		(
			SELECT 
			  wt2.[Database_Name]
			, wt2.[Schema_Name]
			, wt2.Table_Name
			, wt2.Index_Name
			, wt2.Partition_Number
			, wt2.String_Priority_Helper
			, wt2.Calculated_Do_REBUILD
			, wt2.Calculated_Priority
			, rn = ROW_NUMBER() OVER (ORDER BY wt2.String_Priority_Helper DESC)
			FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process AS wt2 WITH (FORCESEEK)
			INNER JOIN ConsultantBux AS R
				ON R.String_Priority_Helper > wt2.String_Priority_Helper
			WHERE wt2.In_Progress = 0
			AND wt2.Calculated_Priority > 0
		) AS R
		WHERE R.rn = 1
	)
	SELECT TOP (1)
	  @database_name_OUT = c.[Database_Name]
	, @schema_name_OUT = c.[Schema_Name]
	, @table_name_OUT = c.Table_Name
	, @index_name_OUT = c.Index_Name
	, @partition_number_OUT = c.Partition_Number
	, @string_priority_helper_OUT = c.String_Priority_Helper
	, @calculated_do_REBUILD_OUT = c.Calculated_Do_REBUILD
	, @calculated_priority_OUT = c.Calculated_Priority
	FROM ConsultantBux c
	WHERE NOT EXISTS (
		SELECT 1
		FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process wt WITH (FORCESEEK)
		WHERE wt.In_Progress = 1
		AND wt.[Database_Name] = c.[Database_Name] AND wt.[Schema_Name] = c.[Schema_Name] AND wt.Table_Name = c.Table_Name
	)
	OPTION (MAXRECURSION 0, USE HINT(''FORCE_DEFAULT_CARDINALITY_ESTIMATION''))';

	SET @parm_definition = N'@database_name_OUT SYSNAME OUTPUT, @schema_name_OUT SYSNAME OUTPUT, @table_name_OUT SYSNAME OUTPUT, @index_name_OUT SYSNAME OUTPUT'
	+ N', @partition_number_OUT INT OUTPUT, @string_priority_helper_OUT VARCHAR(58) OUTPUT, @calculated_do_REBUILD_OUT BIT OUTPUT, @calculated_priority_OUT NUMERIC(38, 0) OUTPUT';

	EXEC sp_executesql @dynamic_sql_max,
	@parm_definition,
	@database_name_OUT = @database_name OUTPUT,
	@schema_name_OUT = @schema_name OUTPUT,
	@table_name_OUT = @table_name OUTPUT,
	@index_name_OUT = @index_name OUTPUT,
	@partition_number_OUT = @partition_number OUTPUT,
	@string_priority_helper_OUT = @string_priority_helper OUTPUT,
	@calculated_do_REBUILD_OUT = @calculated_do_REBUILD OUTPUT,
	@calculated_priority_OUT = @calculated_priority OUTPUT;

	SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process
	SET In_Progress = 1
	, Job_Number_That_Attempted_Alter = @job_number
	WHERE [Database_Name] = @database_name
	AND [Schema_Name] = @schema_name
	AND [Table_Name] = @table_name
	AND Index_Name = @index_name
	AND Partition_Number = @partition_number
	AND In_Progress = 0
	AND String_Priority_Helper = @string_priority_helper';
	
	EXEC sp_executesql @dynamic_sql_max,
	N'@job_number SMALLINT, @database_name SYSNAME, @schema_name SYSNAME, @table_name SYSNAME, @index_name SYSNAME, @partition_number INT, @string_priority_helper VARCHAR(58)',
	@job_number = @job_number,
	@database_name = @database_name,
	@schema_name = @schema_name,
	@table_name = @table_name,
	@index_name = @index_name,
	@partition_number = @partition_number,
	@string_priority_helper = @string_priority_helper;

	EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';	
	COMMIT TRANSACTION;
	
	-- if database is NULL then there is no longer any work for this session to do
	IF @database_name IS NOT NULL
	BEGIN				
		BEGIN TRANSACTION;
		EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';

		SET @alter_start_time = SYSUTCDATETIME();

		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'INSERT INTO ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History
		(Summary_Start_Time_UTC, Job_Number, [Database_Name], [Schema_Name], Table_Name, Index_Name, Partition_Number, Alter_Start_Time_UTC, Session_Id, Was_First_Alter_Of_Run, Was_Rebuild, Calculated_Priority)
		VALUES
		(@parent_start_time, @job_number, @database_name, @schema_name, @table_name, @index_name, @partition_number, @alter_start_time, @@SPID, @first_alter, @calculated_do_REBUILD, @calculated_priority)';

		SET @parm_definition = N'@parent_start_time DATETIME2, @job_number SMALLINT, @database_name SYSNAME, @schema_name SYSNAME, @table_name SYSNAME, @index_name SYSNAME'
		+ N', @partition_number INT, @alter_start_time DATETIME2, @first_alter BIT, @calculated_do_REBUILD BIT, @calculated_priority NUMERIC(38, 0)';
		
		EXEC sp_executesql @dynamic_sql_max,
		@parm_definition,
		@parent_start_time = @parent_start_time,
		@job_number = @job_number,
		@database_name = @database_name,
		@schema_name = @schema_name,
		@table_name = @table_name,
		@index_name = @index_name,
		@partition_number = @partition_number,
		@alter_start_time = @alter_start_time,
		@first_alter = @first_alter,
		@calculated_do_REBUILD = @calculated_do_REBUILD,
		@calculated_priority = @calculated_priority;

		EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';	
		COMMIT TRANSACTION;

		IF @calculated_do_REBUILD = 0
		BEGIN
			SET @alter_sql = N'ALTER INDEX ' + QUOTENAME(@index_name) + ' ON '
			+ QUOTENAME(@database_name) + N'.' + QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name) + N' REORGANIZE '
			+ CASE WHEN @partition_number <> -1 THEN N'PARTITION = ' + CAST(@partition_number AS NVARCHAR(10)) ELSE N'' END
			+ N' WITH (COMPRESS_ALL_ROW_GROUPS = ' + CASE WHEN @reorg_use_COMPRESS_ALL_ROWGROUPS_option = 1 THEN N'ON' ELSE N'OFF' END + N')';
		END
		ELSE
		BEGIN
			SET @alter_sql = N'ALTER INDEX ' + QUOTENAME(@index_name) + ' ON '
			+ QUOTENAME(@database_name) + N'.' + QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name) + N' REBUILD '
			+ CASE WHEN @partition_number <> -1 THEN N'PARTITION = ' + CAST(@partition_number AS NVARCHAR(10)) ELSE N'' END
			+ N' WITH (ONLINE = ' + CASE WHEN @rebuild_ONLINE_option = 1 THEN N'ON' ELSE N'OFF' END 
			+ CASE WHEN @rebuild_MAXDOP IS NOT NULL THEN N', MAXDOP = ' + CAST(@rebuild_MAXDOP AS NVARCHAR(10)) ELSE N'' END			
			+ N')';
		END;

		SELECT @cpu_time_snapshot = cpu_time
		FROM sys.dm_exec_requests
		WHERE session_id = @@SPID;
		
		SET @error_message = NULL;
		BEGIN TRY
			IF @reorg_execute_twice = 1 AND @calculated_do_REBUILD = 0
			BEGIN
				EXEC sp_executesql @alter_sql;
				EXEC sp_executesql @alter_sql;
			END
			ELSE
			BEGIN
				EXEC sp_executesql @alter_sql;
			END;
		END TRY
		BEGIN CATCH
			SET @error_message = ERROR_MESSAGE();
		END CATCH;
		
		SELECT @cpu_time_snapshot = cpu_time - @cpu_time_snapshot
		FROM sys.dm_exec_requests
		WHERE session_id = @@SPID;

		BEGIN TRANSACTION;
		EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';
		
		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History
		SET 
		  Alter_Complete_Time_UTC = CASE WHEN @error_message IS NULL THEN SYSUTCDATETIME() ELSE NULL END
		, Alter_Stop_Time_UTC = CASE WHEN @error_message IS NOT NULL THEN SYSUTCDATETIME() ELSE NULL END
		, Alter_Attempt_CPU_MS = @cpu_time_snapshot
		, Error_Text = @error_message
		, Did_Complete = CASE WHEN @error_message IS NULL THEN 1 ELSE 0 END
		, Did_Error = CASE WHEN @error_message IS NOT NULL THEN 1 ELSE 0 END
		, Did_Stop = CASE WHEN @error_message IS NOT NULL THEN 1 ELSE 0 END
		WHERE Alter_Start_Time_UTC = @alter_start_time
		AND Job_Number = @job_number
		AND Session_Id = @@SPID';

		EXEC sp_executesql @dynamic_sql_max,
		N'@error_message NVARCHAR(4000), @cpu_time_snapshot INT, @alter_start_time DATETIME2, @job_number SMALLINT',
		@error_message = @error_message,
		@cpu_time_snapshot = @cpu_time_snapshot,
		@alter_start_time = @alter_start_time,
		@job_number = @job_number;

		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'DELETE FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process
		WHERE In_Progress = 1
		AND [Database_Name] = @database_name
		AND [Schema_Name] = @schema_name
		AND Table_Name = @table_name
		AND Index_Name = @index_name
		AND Partition_Number = @partition_number
		AND String_Priority_Helper = @string_priority_helper';
	
		EXEC sp_executesql @dynamic_sql_max,
		N'@database_name SYSNAME, @schema_name SYSNAME, @table_name SYSNAME, @index_name SYSNAME, @partition_number INT, @string_priority_helper VARCHAR(58)',
		@database_name = @database_name,
		@schema_name = @schema_name,
		@table_name = @table_name,
		@index_name = @index_name,
		@partition_number = @partition_number,
		@string_priority_helper = @string_priority_helper;

		EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';		
		COMMIT TRANSACTION;

		EXEC [dbo].AgentJobMultiThread_ShouldChildJobHalt 
			@workload_identifier = @workload_identifier,
			@logging_database_name = @logging_database_name,
			@logging_schema_name = @logging_schema_name,
			@parent_start_time = @parent_start_time,
			@should_job_halt_OUT = @should_job_stop OUTPUT;

		IF @should_job_stop = 1
		BEGIN
			RETURN;
		END;
	END
	ELSE
	BEGIN
		-- table is either empty or it wouldn't be possible to run a REORG on any remaining partition due to SQL Server locks
		-- that is because it is not possible to do REORGs simultaneously on different partitions for the same table
		SET @all_partitions_processed = 1;	
	END;
	
	-- @first_alter should be 1 only for the first loop of the procedure
	-- we care about this because we want to know if a previously halted reorg or rebuild had the full maintainence window to run
	SET @first_alter = 0;
END;

END;

GO






CREATE OR ALTER PROCEDURE [dbo].[CCI_Reorg_Rebuild_Cleanup_Jobs] (
	@logging_database_name SYSNAME,
	@logging_schema_name SYSNAME,
	@parent_start_time DATETIME2,
	@max_minutes_to_run SMALLINT
)
AS
BEGIN
/*
Procedure Name: CCI_Reorg_Rebuild_Cleanup_Jobs
Author: Joe Obbish
Version: 1.0
Updates: https://github.com/jobbish-sql/SQL-Server-Multi-Thread
License: MIT
Purpose: To be called by the multithreading job framework. You should not be executing this stored procedure yourself.
*/
DECLARE @workload_identifier NVARCHAR(50) = N'CCI_Reorg_Rebuild',
@stop_jobs BIT,
@custom_procedure_error_message NVARCHAR(4000),
@cleanup_error_message NVARCHAR(4000),
@error_count_found_during_this_procedure INT = 0,
@error_count_from_cleanup INT,
@end_stored_procedure_name_to_run SYSNAME,
@dynamic_sql_max NVARCHAR(MAX) = CAST(N'' AS NVARCHAR(MAX)),
@error_message NVARCHAR(4000),
@used_job_prefix NVARCHAR(20),
@nice_error_message NVARCHAR(4000),
@code_schema_name SYSNAME = OBJECT_SCHEMA_NAME(@@PROCID);

SET NOCOUNT ON;

-- get needed parameter values from the summary table for this run
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'SELECT
  @end_stored_procedure_name_to_run_OUT = End_Stored_Procedure_Name_To_Run
, @used_job_prefix_OUT = Used_Job_Prefix 
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
WHERE Summary_Start_Time_UTC = @parent_start_time';

EXEC sp_executesql @dynamic_sql_max,
N'@end_stored_procedure_name_to_run_OUT SYSNAME OUTPUT, @used_job_prefix_OUT NVARCHAR(100) OUTPUT, @parent_start_time DATETIME2',
@end_stored_procedure_name_to_run_OUT = @end_stored_procedure_name_to_run OUTPUT,	
@used_job_prefix_OUT = @used_job_prefix OUTPUT,
@parent_start_time = @parent_start_time;


-- there was likely a problem with the parent procedure if this is NULL
IF @used_job_prefix IS NULL
BEGIN
	THROW 70010, 'Cannot find expected row in CCI_Reorg_Rebuild_Summary table. Look for an error logged by the CCIReorgAndRebuild stored procedure.', 1; 
	RETURN;
END;
   	  

EXEC [dbo].AgentJobMultiThread_ShouldCleanupStopChildJobs
	@workload_identifier = @workload_identifier,
	@parent_start_time = @parent_start_time,
	@job_prefix = @used_job_prefix,
	@max_minutes_to_run = @max_minutes_to_run,
	@should_stop_jobs_OUT = @stop_jobs OUTPUT;


IF @stop_jobs = 0
BEGIN
	EXEC [dbo].AgentJobMultiThread_FinalizeCleanup
		@workload_identifier = @workload_identifier,
		@job_prefix = @used_job_prefix,
		@retry_cleanup = 1;

	RETURN;
END;


-- save off cpu time to try to update the CCI_Reorg_Rebuild_Index_History table for stopped jobs
CREATE TABLE #cpu_time_by_session_id (session_id INT NOT NULL, cpu_time INT);

INSERT INTO #cpu_time_by_session_id (session_id, cpu_time)
SELECT r.session_id, r.cpu_time
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
WHERE s.is_user_process = 1
OPTION (MAXDOP 1);

EXEC [dbo].AgentJobMultiThread_CleanupChildJobs
	@workload_identifier = @workload_identifier,
	@logging_database_name = @logging_database_name,
	@logging_schema_name = @logging_schema_name,
	@job_prefix = @used_job_prefix,
	@child_job_error_count_OUT = @error_count_from_cleanup OUTPUT,
	@cleanup_error_message_OUT = @cleanup_error_message OUTPUT;


BEGIN TRANSACTION;
EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';

-- make a reasonable effort to update cpu time and other columns for jobs that were likely stopped
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'UPDATE hist
SET hist.Alter_Stop_Time_UTC = SYSUTCDATETIME()
, hist.Did_Complete = 0
, hist.Did_Error = 0
, hist.Did_Stop = 1
, hist.Alter_Attempt_CPU_MS = (
	SELECT ISNULL(MAX(cpu_time), 0)
	FROM #cpu_time_by_session_id s
	WHERE s.session_id = hist.Session_Id
)
- (
	SELECT ISNULL(SUM(Alter_Attempt_CPU_MS), 0)
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History h
	WHERE h.Session_Id = hist.Session_Id
	AND h.Alter_Start_Time_UTC >= @parent_start_time
	AND h.Summary_Start_Time_UTC = @parent_start_time
)
FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History hist
WHERE hist.Alter_Start_Time_UTC >= @parent_start_time
AND hist.Summary_Start_Time_UTC = @parent_start_time
AND hist.Alter_Stop_Time_UTC IS NULL 
AND hist.Alter_Complete_Time_UTC IS NULL';

EXEC sp_executesql @dynamic_sql_max,
N'@parent_start_time DATETIME2',
@parent_start_time = @parent_start_time;

-- purge rows from permanent tables older than 100 days
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'DELETE FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History WITH (TABLOCK)
WHERE Alter_Start_Time_UTC < DATEADD(DAY, -100, SYSUTCDATETIME())';

EXEC sp_executesql @dynamic_sql_max;

EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';
COMMIT TRANSACTION;

-- purge rows from permanent tables older than 100 days
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'DELETE FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary WITH (TABLOCK)
WHERE Summary_Start_Time_UTC < DATEADD(DAY, -100, SYSUTCDATETIME())';

EXEC sp_executesql @dynamic_sql_max;

-- run the custom end procedure, if set
IF @end_stored_procedure_name_to_run IS NOT NULL
BEGIN
	BEGIN TRY
		SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  QUOTENAME(@code_schema_name) + N'.' + QUOTENAME(@end_stored_procedure_name_to_run);
		EXEC sp_executesql @dynamic_sql_max;
	END TRY
	BEGIN CATCH
		SET @custom_procedure_error_message = ERROR_MESSAGE();
		SET @error_count_found_during_this_procedure = @error_count_found_during_this_procedure + 1;
	END CATCH;	
END;

-- add in errors from child jobs
SET @error_count_found_during_this_procedure = @error_count_found_during_this_procedure + @error_count_from_cleanup;

BEGIN TRANSACTION;
EXEC sp_getapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES', @LockMode = 'Exclusive';

-- update summary table
SET @dynamic_sql_max = CAST(N'' AS NVARCHAR(MAX)) +  N'UPDATE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary
SET Summary_End_Time_UTC = SYSUTCDATETIME()
, Alter_Statements_Completed = h.Alter_Statements_Completed
, Alter_Statements_Halted = h.Alter_Statements_Halted
, Alter_Statements_Not_Started = p.Alter_Statements_Not_Started
, Alter_Statements_With_Priority_Zero = p.Alter_Statements_With_Priority_Zero
, Approximate_Error_Count = @error_count_found_during_this_procedure + h.Alter_Statements_Error
, Alter_Total_CPU_MS = h.Alter_Total_CPU_MS
, Custom_End_Procedure_Name_Error_Text = @custom_procedure_error_message
, Cleanup_Error_Text = @cleanup_error_message
FROM (
	SELECT
	  COUNT_BIG(hist.Alter_Complete_Time_UTC) Alter_Statements_Completed
	, COUNT_BIG(hist.Alter_Stop_Time_UTC) Alter_Statements_Halted
	, COUNT_BIG(hist.Error_Text) Alter_Statements_Error
	, SUM(1.0 * hist.Alter_Attempt_CPU_MS) Alter_Total_CPU_MS
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Index_History hist
	WHERE hist.Alter_Start_Time_UTC >= @parent_start_time
	AND hist.Summary_Start_Time_UTC = @parent_start_time
) h
CROSS JOIN 
(
	SELECT
	  SUM(CASE WHEN In_Progress = 0 THEN 1 ELSE 0 END) Alter_Statements_Not_Started
	, SUM(CASE WHEN In_Progress = -1 THEN 1 ELSE 0 END) Alter_Statements_With_Priority_Zero
	FROM ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Partitions_To_Process WITH (FORCESEEK)
	WHERE In_Progress IN (0, -1)
) p
WHERE ' + QUOTENAME(@logging_database_name) + N'.' + QUOTENAME(@logging_schema_name) + N'.CCI_Reorg_Rebuild_Summary.Summary_Start_Time_UTC = @parent_start_time';

EXEC sp_executesql @dynamic_sql_max,
N'@error_count_found_during_this_procedure INT, @custom_procedure_error_message NVARCHAR(4000), @cleanup_error_message NVARCHAR(4000), @parent_start_time DATETIME2',
@error_count_found_during_this_procedure = @error_count_found_during_this_procedure,
@custom_procedure_error_message = @custom_procedure_error_message,
@cleanup_error_message = @cleanup_error_message,
@parent_start_time = @parent_start_time;

EXEC sp_releaseapplock @Resource = N'UPDATE_CCI_REORG_REBUILD_TABLES';
COMMIT TRANSACTION;


EXEC [dbo].AgentJobMultiThread_FinalizeCleanup
	@workload_identifier = @workload_identifier,
	@job_prefix = @used_job_prefix,
	@retry_cleanup = 0;

END;

GO
