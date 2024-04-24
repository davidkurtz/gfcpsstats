REM locktemprecstats2.sql
REM (c)David Kurtz, Go-Faster Consultancy 2009-24
CLEAR SCREEN
set serveroutput on timi on pages 999
@@locktemprecstats_pkg.sql
spool locktemprecstats2
--------------------------------------------------------------------------------------------------------------
--query to retreive list of tables where stats to be locked/unlocked/deleted
--------------------------------------------------------------------------------------------------------------
set pages 99 lines 150 timi on autotrace off trimspool on
column recname format a15
column rectype heading 'Rec|Type' format 99
column table_name format a18
column lock_stats heading 'Lock|Stats' format a5
column num_rows heading 'Num|Rows'
column stattype_locked heading 'Stattype|Locked' format a8
ttitle 'Unlocked Temporary Tables'
select * from table(locktemprecstats.unlockedtables());
ttitle off
--------------------------------------------------------------------------------------------------------------
--procedure to submit jobs to lock/unlock/delete stats
--this could be scheduled periodically
--------------------------------------------------------------------------------------------------------------
EXEC locktemprecstats.locktables;
--------------------------------------------------------------------------------------------------------------
--queries on job scheduler tables to observe progress of jobs
--jobs that are scheduled to be run - as they complete the jobs are autodroped and the rows disappear from this view
--------------------------------------------------------------------------------------------------------------
column job_name format a30
select job_name, start_date, job_action from all_Scheduler_jobs where job_name like '%LOCK%' order by start_date fetch first 100 rows only;
--select * from all_Scheduler_job_log where job_name like 'LOCK%' order by log_id desc;
--------------------------------------------------------------------------------------------------------------
--last 10 jobs that have been run
--------------------------------------------------------------------------------------------------------------
select log_id, log_date, job_name, run_duration, cpu_used
from all_Scheduler_job_run_details where job_name like '%LOCK%' order by log_id desc fetch first 100 rows only;
--------------------------------------------------------------------------------------------------------------
--queries on ASH data to observe performance of code
--------------------------------------------------------------------------------------------------------------
select module, sum(usecs_per_row)/1e6 ash_Secs
from gv$active_Session_History
where sample_time > sysdate - 30/1440 --last 30 minutes
and (module like 'LOCK%' OR action like 'LOCK%')
group by module --, action
/