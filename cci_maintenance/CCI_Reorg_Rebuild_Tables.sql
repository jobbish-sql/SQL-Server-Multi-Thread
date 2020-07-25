-- feel free to change the schema as long as it is consistent
CREATE TABLE dbo.CCI_Reorg_Rebuild_Summary (
	Summary_Start_Time_UTC DATETIME2 NOT NULL,
	Summary_End_Time_UTC DATETIME2 NULL,
	Alter_Statements_Completed INT NULL,
	Alter_Statements_Halted INT NULL,
	Alter_Statements_Not_Started INT NULL,
	Alter_Statements_With_Priority_Zero INT NULL,
	Alter_Total_CPU_MS BIGINT NULL,
	Approximate_Error_Count INT NULL,
	Custom_Start_Procedure_Name_Error_Text NVARCHAR(4000) NULL,
	Custom_End_Procedure_Name_Error_Text NVARCHAR(4000) NULL,
	Cleanup_Error_Text NVARCHAR(4000) NULL,
	CCI_Included_Database_Name_List NVARCHAR(4000) NOT NULL,
	CCI_Excluded_Schema_Name_List NVARCHAR(4000) NULL,
	CCI_Excluded_Table_Name_List NVARCHAR(4000) NULL,
	Max_CCI_Alter_Job_Count SMALLINT NULL,
	Max_Minutes_To_Run SMALLINT NOT NULL,
	Partition_Priority_Algorithm_Name NVARCHAR(100),
	Used_SQL_Expression_For_Partition_Priority_Calculation NVARCHAR(4000) NULL,
	Rebuild_Algorithm_Name NVARCHAR(100),
	Used_SQL_Expression_For_Rebuild_Calculation NVARCHAR(4000) NULL,
	Ignore_Archive_Compressed_Partitions BIT NOT NULL,
	Reorg_Use_COMPRESS_ALL_ROWGROUPS_Option BIT NOT NULL,
	Reorg_Execute_Twice BIT NOT NULL,
	Rebuild_MAXDOP SMALLINT NULL,
	Rebuild_ONLINE_Option BIT NOT NULL,
	Start_Stored_Procedure_Name_To_Run SYSNAME NULL,
	End_Stored_Procedure_Name_To_Run SYSNAME NULL,
	Disable_CPU_Rescheduling BIT NULL,
	Delimiter_Override NVARCHAR(1) NULL,
	Used_Job_Prefix NVARCHAR(100) NOT NULL,
	Prioritization_Only BIT NULL,
	query_CCI_DMV_info BIT NOT NULL
);

CREATE CLUSTERED INDEX CI_CCI_Reorg_Rebuild_Summary ON dbo.CCI_Reorg_Rebuild_Summary (Summary_Start_Time_UTC);

CREATE TABLE dbo.CCI_Reorg_Rebuild_Index_History (
	Summary_Start_Time_UTC DATETIME2 NOT NULL,
	Job_Number SMALLINT NOT NULL,
	Database_Name SYSNAME NOT NULL,
	[Schema_Name] SYSNAME NOT NULL,
	Table_Name SYSNAME NOT NULL,
	Index_Name SYSNAME NOT NULL,
	Partition_Number INT NOT NULL,
	Was_Rebuild INT NOT NULL,
	Alter_Start_Time_UTC DATETIME2 NOT NULL,
	Alter_Stop_Time_UTC DATETIME2 NULL,
	Alter_Complete_Time_UTC DATETIME2 NULL,
	Session_Id INT NOT NULL,
	Was_First_Alter_Of_Run BIT NOT NULL,
	Did_Complete BIT NULL,
	Did_Error BIT NULL,
	Did_Stop BIT NULL,
	Calculated_Priority NUMERIC(38, 0) NULL,
	Alter_Attempt_CPU_MS INT NULL,
	Error_Text NVARCHAR(4000) NULL
);

CREATE CLUSTERED INDEX CI_CCI_Reorg_Rebuild_Index_History ON dbo.CCI_Reorg_Rebuild_Index_History (Alter_Start_Time_UTC, Job_Number);

CREATE NONCLUSTERED INDEX NCI_CCI_Reorg_Rebuild_Index_History ON dbo.CCI_Reorg_Rebuild_Index_History (
Was_First_Alter_Of_Run,
Did_Complete,
Did_Error,
Did_Stop,
[Database_Name],
[Schema_Name],
Table_Name,
Index_Name,
Partition_Number,
Alter_Start_Time_UTC 
) INCLUDE (Was_Rebuild);
