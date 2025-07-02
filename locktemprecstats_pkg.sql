REM locktemprecstats_pkg.sql
REM (c)David Kurtz, Go-Faster Consultancy 2009-24
REM https://www.psftdba.com/gfcpsstats11.pdf
set serveroutput on timi on pages 999 timi on
spool locktemprecstats_pkg
--------------------------------------------------------------------------------------------------------------
-- object definition required by package
--------------------------------------------------------------------------------------------------------------
DROP TYPE SYSADM.t_lock_tab;
DROP TYPE SYSADM.t_lock_row;

CREATE TYPE SYSADM.t_lock_row AS OBJECT 
(recname         VARCHAR2(18 CHAR)
,rectype         INTEGER
,table_name      VARCHAR2(30 CHAR)
,last_analyzed   DATE
,num_rows        INTEGER
,stattype_locked VARCHAR2(5 CHAR)
,lock_stats      VARCHAR2(1 CHAR)
)
/
CREATE TYPE t_lock_tab IS TABLE OF SYSADM.t_lock_row
/
--------------------------------------------------------------------------------------------------------------
-- Package Header
--------------------------------------------------------------------------------------------------------------
create or replace package SYSADM.locktemprecstats as 
function unlockedtables RETURN t_lock_tab PIPELINED;
procedure locktables;
end locktemprecstats;
/
show errors
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------
-- Package Body
--------------------------------------------------------------------------------------------------------------
create or replace package body SYSADM.locktemprecstats as 
--------------------------------------------------------------------------------------------------------------
--Constants that should not be changed
--------------------------------------------------------------------------------------------------------------
k_module           CONSTANT VARCHAR2(64 CHAR) := $$PLSQL_UNIT; --name of package for instrumentation
--------------------------------------------------------------------------------------------------------------
-- unlockedtables returns query of tables whose statistics should be locked and deleted
--------------------------------------------------------------------------------------------------------------
function unlockedtables RETURN t_lock_tab PIPELINED IS
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.set_module(k_module, 'unlockedtables');

  FOR i IN (
    WITH v AS (SELECT /*+MATERIALIZE*/ rownum row_number FROM dual CONNECT BY LEVEL <= 100
    ), x as (
    SELECT /*+MATERIALIZE*/ DISTINCT r.recname, r.rectype, g.lock_stats
    ,      DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name
    FROM   psrecdefn r
    ,      ps_gfc_stats_ovrd g
    WHERE  r.rectype = 0
    AND    g.recname = r.recname
    UNION ALL
    SELECT /*+LEADING(o r i v)*/ DISTINCT r.recname, r.rectype, NVL(g.lock_stats,'Y') lock_stats
    ,      DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)||DECODE(v.row_number*r.rectype,100,'',LTRIM(TO_NUMBER(v.row_number))) table_name
    FROM   psrecdefn r
      LEFT OUTER JOIN ps_gfc_stats_ovrd g ON g.recname = r.recname
    ,      pstemptblcntvw i
    ,      psoptions o
    ,      v
    WHERE  r.rectype = 7
    AND    r.recname = i.recname
    AND    v.row_number <= i.temptblinstances + o.temptblinstances
    )
    SELECT x.recname, x.rectype, t.table_name, t.last_analyzed, t.num_rows, s.stattype_locked, x.lock_stats
    FROM x
      INNER JOIN user_tables t	ON t.temporary = 'N' AND t.table_name = x.table_name
	     LEFT OUTER JOIN user_tab_statistics s ON s.table_name = t.table_name AND s.partition_name IS NULL
    WHERE  (  (x.lock_stats = 'Y' AND s.stattype_locked IS NULL) --stats not locked
           OR (x.lock_stats = 'N' AND s.stattype_locked IS NOT NULL))
    ORDER BY 1,3
    --FETCH FIRST 100 ROWS ONLY
  ) LOOP
    PIPE ROW(t_lock_row(i.recname, i.rectype, i.table_name, i.last_analyzed, i.num_rows, i.stattype_locked, i.lock_stats));   
  END LOOP;

  dbms_application_info.set_module(l_module, l_action);
  RETURN;
END unlockedtables;
--------------------------------------------------------------------------------------------------------------
-- Using the rowset pipeline function above create DB jobs to lock and delete stats
--------------------------------------------------------------------------------------------------------------
PROCEDURE locktables IS
  l_sql CLOB;
  l_job_name VARCHAR2(30);
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.set_module(k_module, 'locktables');
  FOR i IN (select * from table(locktemprecstats.unlockedtables())) LOOP
    dbms_application_info.set_action('locktables.'||i.table_name);
    l_sql := '';
    l_job_name := '';
    IF i.lock_stats = 'N' THEN
      l_sql := l_sql || 'dbms_stats.unlock_table_stats(ownname=>'''||user||''',tabname=>'''||i.table_name||');';
      l_job_name := l_job_name||'UNLOCK_';
    ELSE
      IF i.stattype_locked IS NULL THEN --lock stats
        l_job_name := l_job_name||'LOCK_';
        l_sql := l_sql || 'dbms_stats.lock_table_stats(ownname=>'''||user||''',tabname=>'''||i.table_name||''');';
      END IF;
      IF i.last_analyzed IS NOT NULL THEN --delete stats
        l_job_name := l_job_name||'DELETE_';
        l_sql := l_sql || 'dbms_stats.delete_table_stats(ownname=>'''||user||''',tabname=>'''||i.table_name||''',force=>TRUE);';
      END IF;
    END IF;
    IF l_sql IS NOT NULL THEN
      --dbms_output.put_line(l_sql);
      DBMS_SCHEDULER.create_job(
        job_name        => l_job_name||i.table_name,
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN '||l_sql||' END;',
        start_date      => SYSTIMESTAMP,
        auto_drop       => TRUE,
        enabled         => TRUE);
    END IF;
  END LOOP;
  dbms_application_info.set_module(l_module, l_action);
END locktables;
--------------------------------------------------------------------------------------------------------------
end locktemprecstats;
/
show errors
spool off

