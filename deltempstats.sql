rem deltempstats.sql
rem (c)Go-Faster Consultancy 2009-21
rem 02.07.2009 Trigger to delete CBO statistics when instance of a temporary table is allocated to process
rem 26.08.2009 Trigger now fire when table is allocated to new process instance, and only deletes statistics if they exist.
rem 21.11.2009 added test to lock statistics if not already locked
rem 23.06.2014 adjusted to remove outer join and restrict to type 7 records
rem 11.03.2021 added exception to locking settings

set echo on pause off
clear screen
spool deltempstats
rollback;

CREATE OR REPLACE TRIGGER sysadm.gfc_deletetemptablestats
AFTER INSERT ON sysadm.ps_aetemptblmgr
FOR EACH ROW
WHEN (new.curtempinstance > 0)
DECLARE
 PRAGMA AUTONOMOUS_TRANSACTION;
 l_table_name      user_tab_statistics.table_name%TYPE := '';
 l_last_analyzed   user_tab_statistics.last_analyzed%TYPE := '';
 l_stattype_locked user_tab_statistics.stattype_locked%TYPE := '';
 l_rectype         INTEGER;
 l_lock_stats      ps_gfc_stats_ovrd.lock_stats%TYPE := '';
 table_doesnt_exist EXCEPTION;
 PRAGMA EXCEPTION_INIT(table_doesnt_exist,-20001);
BEGIN
 SELECT t.table_name, t.last_analyzed, s.stattype_locked, r.rectype, g.lock_stats
 INTO   l_table_name, l_last_analyzed, l_stattype_locked, l_rectype, l_lock_stats
 FROM	psrecdefn r
   LEFT OUTER JOIN ps_gfc_stats_ovrd g
   ON g.recname = r.recname
 ,      user_tables t
   LEFT OUTER JOIN user_tab_statistics s
   ON  s.table_name = t.table_name
   AND s.partition_name IS NULL
   AND s.object_type = 'TABLE'
 WHERE  r.recname = :new.recname
 AND    r.rectype = 7
 AND    t.temporary = 'N'
 AND    t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)||NULLIF(:new.curtempinstance,0)
 ;

 IF l_lock_stats = 'Y' OR (l_rectype = 7 AND l_lock_stats IS NULL) THEN
   IF l_last_analyzed IS NOT NULL THEN --only delete statistics if they exist
    dbms_stats.delete_table_stats(ownname=>'SYSADM',tabname=>l_table_name,force=>TRUE);
   END IF;
   IF l_stattype_locked IS NULL THEN --stats need to be locked, 21,11,2009
    dbms_stats.lock_table_stats(ownname=>user,tabname=>l_table_name);
   END IF;
 END IF;
 
EXCEPTION
  WHEN no_data_found THEN NULL;
  WHEN table_doesnt_exist THEN NULL;
END;
/
show errors

rollback;



/*suggested test
/*1. see which table has stats
select t.table_name, t.num_Rows, t.last_analyzed
 FROM	psrecdefn r
 ,      user_tables t
 WHERE  r.recname = 'CA_BI_PC_TA2'
 AND    r.rectype = 7
 AND    t.temporary = 'N'
 AND    t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)||NULLIF(0,0)
/
/*2. insert controll record that should fire the trigger
insert into ps_aetemptblmgr
values (-1,'CA_BI_PC_TA2',4,'dmk','1','DMK',SYSDATE,'Y',0,0)
/
/*3. see if the stats have disappeared
select t.table_name, t.num_Rows, t.last_analyzed
FROM psrecdefn r
 ,      user_tables t
 WHERE  r.recname = 'CA_BI_PC_TA2'
 AND    r.rectype = 7
 AND    t.temporary = 'N'
and t.table_name like  DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)||'%'
/
--4. note rollback will not reinstate the stats
rollback;
*/

spool off
