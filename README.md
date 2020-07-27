Splitting up work to run between multiple SQL Server Agent jobs isn't the easiest thing to do. This framework aims to simmplify the process by abstracting away the Agent job parts. Currently only supported on SQL Server 2016 SP2+ and 2017+.

## core:

You need the core folder if you want to use the framework at all. This folder contains the stored procedures needed for the framework and the workloads in the other folders will not run without it. Key features:

* All Agent job work is abstracted away with a parent, child, and cleanup stored procedure model
* Can specify the maximum number of child stored procedures that can run at the same time
* The cleanup procedure stops all child procedures if they hit the defined time limit
* The child procedures reschedule themselves in an attempt to spread out work over schedulers evenly
* Validation and error reporting

## cci_maintenance:

You want cci_maintenance folder if you need a clustered columnstore maintenance solution designed to work over very databases that provides you as much flexibility and control as possible. You define the priority order for maintenance actions as well as whether or not a partition should go through REORGANIZE or REBUILD. Key features:

* Can create multiple threads to do work concurrently
* Stops all work at time limit
* Supports columnstore indexes in multiple databases
* Over 40 data points available to define the priority order at the partition level for maintenance actions
* Over 40 data points available to choose between REORGANIZE and REBUILD at the partition level
* Saves history of previous runs which can be used for prioritization
* Queries against sys.dm_db_column_store_row_group_physical_stats run multi-threaded and are skipped if possible

Note: REORGANIZE and REBUILD do not preserve segment level ordering within a partition. Tables can be excluded from maintenance actions at the schema, table, or table name pattern matching levels. For a maintenance solution that can perserve segment ordering check out [CISL](https://github.com/NikoNeugebauer/CISL).

## demo:

You want the demo folder if you are developing your own code using the framework and think that looking at a simple example would be helpful. This workload runs make-work stored procedures that calculate checksums.
