REM gfcpsstats11.sql
REM (c) Go-Faster Consultancy 2008-2021
REM https://www.psftdba.com/gfcpsstats11.pdf
REM dbms_stats wrapper script for Oracle 11gR2 PT>=8.48
REM 12. 2.2009 force stats collection on regular tables, skip GTTs
REM 24. 3.2009 added refresh procedure for partitioned objects, gather_table_stats proc for wrapping
REM 26. 5.2009 meta data to control behaviour by PS record
REM 23. 6.2009 collect stats on private instance of GTTs
REM  1.10.2009 change default behaviour if no meta data to gather stats, add extra meta data to suppress stats for rest of TL_TIMEADMIN
REM  2.10.2009 enhanced messages in verbose mode
REM  5. 5.2011 add block sample support
REM 25. 7.2012 cloned from wrapper848.sql and renamed as gfcpsstats11
REM  3. 4.2014 changed default from refresh stale on normal tables to gather
REM  6. 4.2014 added functionality to delete stats
REM 25.04.2014 TRIGGER gfc_stat_ovrd_stored_stmt: replace double semi-colon on end of stored statement with single semi-colon
REM 01.07.2014 added exception handler to gfc_stats_ovrd_create_table to suppress exception when trying to create duplicate jobs
REM 30. 3.2017 force upper case owner and table names - change in behaviour in PT8.55
REM 28.10.2017 apply prefernces to PSY tables to support App Designer alter by recreate, and GFC_ tables for GFC_PSPSART
REM 10.03.2021 enhancements for preference_overrides_parameters and exceptions to table statistics locking 
REM 14.02.2023 specify default values for not null columns, correct update to make APPROX_NDV not nullable
REM 23. 6.2023 refresh table stats if global partition stats and now if not partitioned
clear screen
set echo on serveroutput on lines 180 pages 50 wrap off
spool gfcpsstats11

ROLLBACK --just to be safe
/

------------------------------------------------------------------------------------------------
--This record should be created in Application Designer
--a number of columns have been added for 11g so alter table commands included for upgrade
--CASCASE - is not a part of the metadata becasue we always want to cascade, and is the default
--NOINVALIDATE - is not a part of metadata because we always want to invalidate cursors
------------------------------------------------------------------------------------------------
--DROP TABLE ps_gfc_stats_ovrd PURGE;
--DROP TRIGGER gfc_stats_ovrd_create_table;
CREATE TABLE ps_gfc_stats_ovrd 
(recname          VARCHAR2(15)   NOT NULL --peoplesoft record name
,gather_stats     VARCHAR2(1)    NOT NULL --(G)ather Stats / (R)efresh Stats / Do(N)t Gather Stats / (R)efresh stale Stats
,estimate_percent VARCHAR2(30)   NOT NULL --same as dbms_stats.estimate_percent parameter
,block_sample     VARCHAR2(1)    NOT NULL --same as dbms_stats.block_sample parameter
,method_opt       VARCHAR2(1000) NOT NULL --same as dbms_stats.method_opt parameter
,degree           VARCHAR2(30)   NOT NULL --same as dbms_stats.degree parameter
,granularity      VARCHAR2(30)   NOT NULL --same as dbms_stats.granularity parameter
,incremental      VARCHAR2(5)    NOT NULL --Y/N - same as dbms_stats table preference INCREMENTAL
,stale_percent    NUMBER         NOT NULL --same as dbms_stats table preference STALE_PERCENT
,approx_ndv       VARCHAR2(30)   NOT NULL --same as dbms_Stats table preference APPROXIMATE_NDV_ALGORITHM
,pref_over_param  VARCHAR2(5)    NOT NULL --TRUE/FALSE
,lock_stats       VARCHAR2(1)    NOT NULL --Y/N - lock table stats
,del_stats        VARCHAR2(1)    NOT NULL --Y/N - del table stats if also locking them
) TABLESPACE PTTBL PCTFREE 10 PCTUSED 80
/

ALTER TABLE ps_gfc_stats_ovrd ADD approx_ndv      VARCHAR2(30);
ALTER TABLE ps_gfc_stats_ovrd ADD pref_over_param VARCHAR2(5) /*TRUE/FALSE*/;
ALTER TABLE ps_gfc_stats_ovrd ADD lock_stats      VARCHAR2(1) /*Y/N - lock table stats*/;
ALTER TABLE ps_gfc_stats_ovrd ADD del_stats       VARCHAR2(1) /*Y/N - delete table stats if also locking them*/;

UPDATE ps_gfc_stats_ovrd SET approx_ndv      = ' ' WHERE approx_ndv      IS NULL;
UPDATE ps_gfc_stats_ovrd SET pref_over_param = ' ' WHERE pref_over_param IS NULL;
UPDATE ps_gfc_stats_ovrd SET lock_stats      = ' ' WHERE lock_stats      IS NULL;
UPDATE ps_gfc_stats_ovrd SET del_stats       = ' ' WHERE del_stats       IS NULL;

ALTER TABLE ps_gfc_stats_ovrd MODIFY approx_ndv      NOT NULL;
ALTER TABLE ps_gfc_stats_ovrd MODIFY pref_over_param NOT NULL;
ALTER TABLE ps_gfc_stats_ovrd MODIFY lock_stats      NOT NULL;
ALTER TABLE ps_gfc_stats_ovrd MODIFY del_stats       NOT NULL;

ALTER TABLE ps_gfc_stats_ovrd MODIFY gather_stats     DEFAULT 'G';
ALTER TABLE ps_gfc_stats_ovrd MODIFY estimate_percent DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY block_sample     DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY method_opt       DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY degree           DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY granularity      DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY incremental      DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY stale_percent    DEFAULT 0;
ALTER TABLE ps_gfc_stats_ovrd MODIFY approx_ndv       DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY pref_over_param  DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY lock_stats       DEFAULT ' ';
ALTER TABLE ps_gfc_stats_ovrd MODIFY del_stats        DEFAULT ' ';

CREATE UNIQUE INDEX ps_gfc_stats_ovrd ON ps_gfc_stats_ovrd (recname)
TABLESPACE PSINDEX PCTFREE 10 NOPARALLEL LOGGING
/

------------------------------------------------------------------------------------------------
--This function based index is required on PSRECDEFN to optimize the reverse lookup of the record
--from the table name.  The index cannot be defined in Application Designer
------------------------------------------------------------------------------------------------
CREATE INDEX pszpsrecdefn_fbi 
ON psrecdefn (DECODE(sqltablename,' ','PS_'||recname,sqltablename))
TABLESPACE PSINDEX PCTFREE 0
/


CREATE OR REPLACE PACKAGE gfcpsstats11 AS
------------------------------------------------------------------------------------------------
--procedure called from DDL model for %UpdateStats
--24.3.2009 adjusted to call local dbms_stats AND refresh stats
------------------------------------------------------------------------------------------------
PROCEDURE ps_stats 
(p_ownname      IN VARCHAR2 /*owner of table*/
,p_tabname      IN VARCHAR2 /*table name*/
,p_verbose      IN BOOLEAN DEFAULT FALSE /*if true print SQL*/
);
------------------------------------------------------------------------------------------------
--procedure to set table preferences on named tables
--added 26.7.2012
------------------------------------------------------------------------------------------------
PROCEDURE set_table_prefs
(p_tabname      IN VARCHAR2 /*table name*/
,p_recname      IN VARCHAR2 DEFAULT NULL /*record of table if known*/
);
------------------------------------------------------------------------------------------------
--procedure to set table preferences on tables relating to named record
--added 26.7.2012
------------------------------------------------------------------------------------------------
PROCEDURE set_record_prefs
(p_recname      IN VARCHAR2 /*record name*/
);
------------------------------------------------------------------------------------------------
--procedure to update metadata from actual table preferences
--added 21.9.2012
------------------------------------------------------------------------------------------------
PROCEDURE generate_metadata;
------------------------------------------------------------------------------------------------
END gfcpsstats11;
/
show errors

------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY gfcpsstats11 AS
------------------------------------------------------------------------------------------------
 g_lf VARCHAR2(1) := CHR(10); --line feed character
 table_stats_locked EXCEPTION;
 PRAGMA EXCEPTION_INIT(table_stats_locked,-20005);
------------------------------------------------------------------------------------------------
--emit timestamped message during process
------------------------------------------------------------------------------------------------
PROCEDURE msg
(p_msg VARCHAR2
,p_log BOOLEAN DEFAULT FALSE
) IS
BEGIN
 IF p_log THEN
   psftapi.message_log(p_message=>p_msg,p_verbose=>TRUE);
 ELSE
   dbms_output.put_line(TO_CHAR(SYSDATE,'hh24:mi:ss dd.mm.yyyy')||':'||p_msg);
 END IF;
END msg;

------------------------------------------------------------------------------------------------
--Function to convert boolean to string
------------------------------------------------------------------------------------------------
FUNCTION display_bool
(p_bool BOOLEAN
) RETURN VARCHAR2 IS
BEGIN
 IF p_bool THEN 
  RETURN 'TRUE';
 ELSE
  RETURN 'FALSE';
 END IF;
END display_bool;

------------------------------------------------------------------------------------------------
--gfcpsstats11 for dbms_stats package procedure with own logic
--25.7.2012 removed p_method_opt
------------------------------------------------------------------------------------------------
PROCEDURE gather_table_stats
(p_ownname       IN VARCHAR2 
,p_tabname       IN VARCHAR2 
,p_partname      IN VARCHAR2 DEFAULT NULL
,p_block_sample  IN BOOLEAN  DEFAULT FALSE
,p_degree        IN NUMBER   DEFAULT NULL
,p_granularity   IN VARCHAR2 DEFAULT NULL
,p_cascade       IN BOOLEAN  DEFAULT NULL
,p_stattab       IN VARCHAR2 DEFAULT NULL 
,p_statid        IN VARCHAR2 DEFAULT NULL
,p_statown       IN VARCHAR2 DEFAULT NULL
,p_no_invalidate IN BOOLEAN  DEFAULT NULL
,p_force         IN BOOLEAN  DEFAULT NULL
,p_verbose       IN BOOLEAN  DEFAULT FALSE /*if true print SQL*/
) IS
 l_sql VARCHAR2(4000 CHAR);
BEGIN
 l_sql := 'sys.dbms_stats.gather_table_stats'
          ||g_lf||'(ownname => :p_ownname'
          ||g_lf||',tabname => :p_tabname';
 IF p_partname IS NOT NULL THEN
  l_sql := l_sql||g_lf||',partname => '''||p_partname||'''';
 END IF;

 IF p_block_sample IS NOT NULL THEN
  l_sql := l_sql||g_lf||',block_sample => '||display_bool(p_block_sample);
 END IF;

 IF p_degree IS NOT NULL THEN
  l_sql := l_sql||g_lf||',degree => '||p_degree;
 END IF;

 IF p_granularity IS NOT NULL THEN
  l_sql := l_sql||g_lf||',granularity => '''||p_granularity||'''';
 END IF;

 IF p_cascade IS NOT NULL THEN
  l_sql := l_sql||g_lf||',cascade => '||display_bool(p_cascade);
 END IF;

 IF p_stattab IS NOT NULL THEN
  l_sql := l_sql||g_lf||',stattab => '''||p_stattab||'''';
 END IF;
 IF p_statid IS NOT NULL THEN
  l_sql := l_sql||g_lf||',statid => '''||p_statid||'''';
 END IF;
 IF p_statown IS NOT NULL THEN
  l_sql := l_sql||g_lf||',statown => '''||p_statown||'''';
 END IF;

 IF p_no_invalidate IS NOT NULL THEN
  l_sql := l_sql||g_lf||',no_invalidate => '||display_bool(p_no_invalidate);
 END IF;

 IF p_force IS NOT NULL THEN
  l_sql := l_sql||g_lf||',force => '||display_bool(p_force);
 END IF;

 l_sql := l_sql||');';
 l_sql := 'BEGIN '||l_sql||' END;';

--IF p_verbose THEN
-- msg('Table:'||p_ownname||'.'||p_tabname); 
--END IF;
 msg(l_sql,p_verbose); 

 EXECUTE IMMEDIATE l_sql USING IN p_ownname, p_tabname;
END gather_table_stats;

------------------------------------------------------------------------------------------------
--Procedure to refresh stale stats on table, partition AND subpartition
--24.3.2009 added refresh procedure for partitioned objects, gather_table_stats proc for wrapping
-- 2.4.2009 added flush table monitoring stats to refresh stats pacakge
--28.9.2012 made private - no longer need to call from outside pacakge
------------------------------------------------------------------------------------------------
PROCEDURE refresh_stats
(p_ownname             IN VARCHAR2
,p_tabname             IN VARCHAR2
,p_block_sample        IN BOOLEAN DEFAULT FALSE /*if true block sample stats*/
,p_force               IN BOOLEAN DEFAULT FALSE
,p_verbose             IN BOOLEAN DEFAULT FALSE /*if true print SQL*/
) IS
 l_force VARCHAR(1 CHAR);
BEGIN
 IF p_verbose THEN
  msg('Checking '||p_ownname||'.'||p_tabname||' for stale statistics',p_verbose);
 END IF;

 dbms_stats.flush_database_monitoring_info;

 IF p_force THEN --need this is to be a varchar for SQL
  l_force := 'Y';
 ELSE
  l_force := 'N';
 END IF;

--IF p_verbose THEN --qwert
--  msg('Force Statistics collection: '||l_force,p_verbose);
--END IF;

 FOR i IN (
  SELECT p.table_owner, p.table_name, p.partition_name, p.subpartition_name
  FROM   all_tab_subpartitions p
          LEFT OUTER JOIN all_tab_statistics s
          ON  s.owner = p.table_owner
          AND s.table_name = p.table_name
          AND s.partition_name = p.partition_name
          AND s.subpartition_name = p.subpartition_name
  WHERE  p.table_owner = UPPER(p_ownname) /*30.3.2017 added upper()*/
  AND    p.table_name = UPPER(p_tabname) /*30.3.2017 added upper()*/
  AND    (  s.stattype_locked IS NULL 
         OR l_force = 'Y')
  AND    (  p.num_rows IS NULL
         OR s.stale_stats = 'YES')
 ) LOOP 
--IF p_verbose THEN
--  msg('Gather Stats on '||i.table_owner||'.'||i.table_name||' subpartition ('||i.subpartition_name||') force='||l_force,p_verbose);
--END IF;
  gfcpsstats11.gather_table_stats
  (p_ownname      => i.table_owner
  ,p_tabname      => i.table_name
  ,p_partname     => i.subpartition_name
  ,p_block_sample => p_block_sample
  ,p_cascade      => TRUE
  ,p_granularity  => 'SUBPARTITION'
  ,p_force        => p_force
  ,p_verbose      => p_verbose
  );
 END LOOP;

 FOR i IN (
  SELECT p.table_owner, p.table_name, p.partition_name
  FROM   all_tab_partitions p
         LEFT OUTER JOIN all_tab_statistics s
         ON  s.owner = p.table_owner
         AND s.table_name = p.table_name
         AND s.partition_name = p.partition_name
         AND s.subpartition_name IS NULL
  WHERE  p.table_owner = UPPER(p_ownname) /*30.3.2017 added upper()*/
  AND    p.table_name = UPPER(p_tabname) /*30.3.2017 added upper()*/
  AND    (  s.stattype_locked IS NULL 
         OR l_force = 'Y')
  AND    (  s.global_stats = 'YES'
         OR s.global_stats IS NULL)  
  AND    (  p.num_rows IS NULL
         OR s.stale_stats = 'YES')
 ) LOOP
--IF p_verbose THEN
--  msg('Gather Stats on '||i.table_owner||'.'||i.table_name||' partition ('||i.partition_name||') force='||l_force,p_verbose);
--END IF;
  gfcpsstats11.gather_table_stats
  (p_ownname      => i.table_owner
  ,p_tabname      => i.table_name
  ,p_partname     => i.partition_name
  ,p_block_sample => p_block_sample
  ,p_cascade      => TRUE
  ,p_granularity  => 'PARTITION'
  ,p_force        => p_force
  ,p_verbose      => p_verbose
  );
 END LOOP;

 FOR i IN (
  SELECT p.owner, p.table_name
  FROM   all_tables p
         LEFT OUTER JOIN all_tab_statistics s
         ON  s.owner = p.owner
         AND s.table_name = p.table_name
         AND s.partition_name IS NULL
         AND s.subpartition_name IS NULL
  WHERE  p.owner = upper(p_ownname) /*30.3.2017 added upper()*/
  AND    p.table_name = upper(p_tabname) /*30.3.2017 added upper()*/
  AND    (  s.stattype_locked IS NULL 
         OR l_force = 'Y')
  AND    (  s.global_stats = 'YES' -- 23.6.2023 collect stats at table level if partitioned with global stats
         OR p.partitioned = 'NO')  -- 23.6.2023 collect stats at table level if not partitioned
  AND    (  p.num_rows IS NULL
         OR s.stale_stats = 'YES')
 ) LOOP
--IF p_verbose THEN
--  msg('Gather Stats on '||i.owner||'.'||i.table_name||' force='||l_force,p_verbose);
--END IF;
  gfcpsstats11.gather_table_stats
  (p_ownname      => i.owner
  ,p_tabname      => i.table_name
  ,p_block_sample => p_block_sample
  ,p_cascade      => TRUE
  ,p_granularity  => 'GLOBAL'
  ,p_force        => p_force
  ,p_verbose      => p_verbose
  );
 END LOOP;

--IF p_verbose THEN
-- msg('Table:'||p_ownname||'.'||p_tabname||' finished');
--END IF;

END refresh_stats; 

------------------------------------------------------------------------------------------------
--convert oracle table name to PeopleSoft record name in a way that avoids doing a reverse 
--lookup on psrecdefn where possible.
------------------------------------------------------------------------------------------------
PROCEDURE table_to_recname
(p_tabname         IN     VARCHAR2
,p_recname         IN OUT VARCHAR2
,p_rectype         IN OUT INTEGER
,p_temptblinstance IN OUT INTEGER
,p_msg             IN OUT VARCHAR2
) IS 
 l_tablen  INTEGER;
BEGIN
 l_tablen := LENGTH(p_tabname);

 BEGIN --what is the PeopleSoft record name and type
  SELECT r.rectype, r.recname
  INTO   p_rectype, p_recname
  FROM   psrecdefn r
  WHERE  r.sqltablename IN(' ','PS_'||r.recname)
  AND    ((r.recname = SUBSTR(p_tabname,4) AND SUBSTR(p_tabname,1,3) IN('PS_','PSY'))
  OR      (r.recname = SUBSTR(p_tabname,5) AND SUBSTR(p_tabname,1,4) IN('GFC_')))
  AND    r.rectype IN(0,7);
 EXCEPTION 
  WHEN no_data_found THEN NULL;
 END;
--msg('@1='||p_recname);

 IF p_recname IS NULL THEN --then it might be a temporary record, first try non-shared instances 1-9
  BEGIN --what is the PeopleSoft record name and type
   SELECT r.rectype, r.recname, SUBSTR(p_tabname,-1)
   INTO   p_rectype, p_recname, p_temptblinstance
   FROM   psrecdefn r
   WHERE  r.sqltablename IN(' ','PS_'||r.recname)
   AND    r.recname = SUBSTR(p_tabname,4,l_tablen-4) AND SUBSTR(p_tabname,1,3) IN('PS_','PSY')
   AND    r.rectype = 7;
   p_msg := p_msg||'Instance '||p_temptblinstance||'. ';
  EXCEPTION 
   WHEN no_data_found THEN NULL;
  END;
--msg('@2='||p_recname);
 END IF;

 IF p_recname IS NULL THEN --then it might be a temporary record, first try non-shared instances 1-9
  BEGIN --what is the PeopleSoft record name and type
   SELECT r.rectype, r.recname, SUBSTR(p_tabname,-2)
   INTO   p_rectype, p_recname, p_temptblinstance
   FROM   psrecdefn r
   WHERE  r.sqltablename IN(' ','PS_'||r.recname)
   AND    r.recname = SUBSTR(p_tabname,4,l_tablen-5) AND SUBSTR(p_tabname,1,3) IN('PS_','PSY')
   AND    r.rectype = 7;
   p_msg := p_msg||'Instance '||p_temptblinstance||'. ';
  EXCEPTION 
   WHEN no_data_found THEN NULL;
  END;
--msg('@3='||p_recname);
 END IF;

 --if we still have not got a record then need to do proper reverse lookup with SQL table name
 IF p_recname IS NULL THEN --then sqltablename might be specified 
  BEGIN --what is the PeopleSoft record name and type
   SELECT r.rectype, r.recname
   INTO   p_rectype, p_recname
   FROM   psrecdefn r
   WHERE  DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) = p_tabname
   AND    r.rectype IN(0,7);
  EXCEPTION 
   WHEN no_data_found THEN NULL;
  END;
--msg('@4='||p_recname);
 END IF;

 IF p_recname IS NULL THEN --then it might be a temporary record, first try non-shared instances 1-9
  BEGIN --what is the PeopleSoft record name and type
   SELECT r.rectype, r.recname, SUBSTR(p_tabname,-1)
   INTO   p_rectype, p_recname, p_temptblinstance
   FROM   psrecdefn r
   WHERE  DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) = SUBSTR(p_tabname,1,l_tablen-1)
   AND    r.rectype = 7;

   p_msg := p_msg||'Instance '||p_temptblinstance||'. ';
  EXCEPTION 
   WHEN no_data_found THEN NULL;
  END;
--msg('@5='||p_recname);
 END IF;

 IF p_recname IS NULL THEN --then it might be a temporary record, try non-shared instances 10-99
  BEGIN --what is the PeopleSoft record name and type
   SELECT r.rectype, r.recname, SUBSTR(p_tabname,-2)
   INTO   p_rectype, p_recname, p_temptblinstance
   FROM   psrecdefn r
   WHERE  DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) = SUBSTR(p_tabname,1,l_tablen-2)
   AND    r.rectype = 7;

   p_msg := p_msg||'Instance '||p_temptblinstance||'. ';
  EXCEPTION 
   WHEN no_data_found THEN NULL;
  END;
--msg('@6='||p_recname);
 END IF;

END table_to_recname;

------------------------------------------------------------------------------------------------
--private procedure to set a single preference on a table, or remove it if parameter is null
------------------------------------------------------------------------------------------------
PROCEDURE set_table_pref
(p_tabname VARCHAR2
,p_pname   VARCHAR2
,p_value   VARCHAR2 DEFAULT NULL
) IS
BEGIN
--msg('set_table_pref(tabname='||p_tabname||',pname='||p_pname||',value='||p_value||')');
 IF p_value IS NULL OR p_value = ' ' THEN
  sys.dbms_stats.delete_table_prefs(ownname=>user, tabname=>p_tabname, pname=>p_pname);
 ELSE
  sys.dbms_stats.set_table_prefs(ownname=>user, tabname=>p_tabname, pname=>p_pname, pvalue=>p_value);
 END IF;
END set_table_pref;
------------------------------------------------------------------------------------------------
--public procedure to set table preferences on named tables called from DDL trigger
------------------------------------------------------------------------------------------------
PROCEDURE set_table_prefs
(p_tabname VARCHAR2
,p_recname VARCHAR2 DEFAULT NULL
) IS 
 l_recname         VARCHAR2(30);
 l_rectype         INTEGER;
 l_temptblinstance VARCHAR2(2 CHAR) := '';
 l_msg             VARCHAR2(200 CHAR);
BEGIN
 IF p_recname IS NULL THEN
  table_to_recname(p_tabname, l_recname, l_rectype, l_temptblinstance, l_msg);
 ELSE
  l_recname := p_recname;
 END IF;

--msg('set_table_prefs(tabname=>'||p_tabname||',recname=>'||l_recname||')');

 FOR i IN(
  SELECT r.recname, r.rectype
  ,      NULLIF(o.estimate_percent,' ') estimate_percent
  ,      NULLIF(o.method_opt,' ') method_opt
  ,      NULLIF(o.degree,' ') degree
  ,      NULLIF(o.granularity,' ') granularity
  ,      NULLIF(o.incremental,' ') incremental
  ,      NULLIF(o.stale_percent,0) stale_percent
  ,      NULLIF(o.approx_ndv,' ') approx_ndv
  ,      NULLIF(o.pref_over_param,' ') pref_over_param
  ,      NULLIF(o.lock_stats,' ') lock_stats
  ,      NULLIF(o.del_stats,' ') del_stats
  FROM   user_tables t
  ,      psrecdefn r
    LEFT OUTER JOIN ps_gfc_stats_ovrd o
    ON   o.recname = r.recname
  WHERE  r.recname = l_recname
  AND    (  (t.table_name LIKE DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)||'%')
         OR (t.table_name LIKE DECODE(r.sqltablename,' ','PSY'||r.recname,r.sqltablename)||'%')
         OR (t.table_name LIKE DECODE(r.sqltablename,' ','GFC_'||r.recname,r.sqltablename)||'%'))
  AND    t.table_name = p_tabname
 ) LOOP
  msg('set_table_prefs(recname='||l_recname||',tabname='||p_tabname
    ||',estimate_percent='||i.estimate_percent
    ||',method_opt='||i.method_opt
    ||',degree='||i.degree
    ||',granularity='||i.granularity
    ||',incremental='||i.incremental
    ||',stale_percent='||i.stale_percent
    ||',approx_ndv='||i.approx_ndv
    ||',pref_over_param='||i.pref_over_param
    ||')');

  set_table_pref(p_tabname=>p_tabname, p_pname=>'CASCADE'); --10.3.21 - cascade is not in the metadata so we will unset it 
  set_table_pref(p_tabname=>p_tabname, p_pname=>'ESTIMATE_PERCENT', p_value=>i.estimate_percent);
  set_table_pref(p_tabname=>p_tabname, p_pname=>'METHOD_OPT',       p_value=>i.method_opt);
  set_table_pref(p_tabname=>p_tabname, p_pname=>'DEGREE',           p_value=>i.degree);
  set_table_pref(p_tabname=>p_tabname, p_pname=>'GRANULARITY',      p_value=>i.granularity);
  set_table_pref(p_tabname=>p_tabname, p_pname=>'INCREMENTAL',      p_value=>i.incremental);
  set_table_pref(p_tabname=>p_tabname, p_pname=>'STALE_PERCENT',    p_value=>i.stale_percent);
   
  $if dbms_db_version.ver_le_12_1 $then $else 
    set_table_pref(p_tabname=>p_tabname, p_pname=>'APPROXIMATE_NDV_ALGORITHM'     , p_value=>i.approx_ndv);
    set_table_pref(p_tabname=>p_tabname, p_pname=>'PREFERENCE_OVERRIDES_PARAMETER', p_value=>i.pref_over_param);
  $end
  
  IF i.lock_stats IS NOT NULL THEN --10.3.2021 not a preference by added to metadata
    IF i.lock_stats = 'Y' OR (i.lock_stats IS NULL AND i.rectype = 7) THEN
      sys.dbms_stats.lock_table_stats(ownname=>user, tabname=>p_tabname);
      IF i.del_stats = 'Y' OR (i.del_stats IS NULL AND i.rectype = 7) THEN 
        sys.dbms_stats.delete_table_stats(ownname=>user, tabname=>p_tabname, force=>TRUE);
      END IF;
    ELSIF i.lock_stats = 'N' OR (i.lock_stats IS NULL AND i.rectype = 0) THEN
      sys.dbms_stats.unlock_table_stats(ownname=>user, tabname=>p_tabname);
    END IF;
  END IF;
  
 END LOOP;
END set_table_prefs;

------------------------------------------------------------------------------------------------
--public procedure to set table preferences on tables relating to named record
--note that this time the preferences are passed into the procedure not retrieved from 
--the metadata table because this will be called from the DML trigger on the metadata table
------------------------------------------------------------------------------------------------
PROCEDURE set_record_prefs
(p_recname IN VARCHAR2
) IS 
BEGIN
 msg('set_record_prefs(recname=>'||p_recname||')');
 FOR i IN(
  SELECT r.recname
  ,      t.table_name
  ,      r.rectype
  FROM   user_tables t, psrecdefn r
  WHERE  r.recname LIKE p_recname
  AND    r.rectype IN(0,7)
  AND    (  t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
         OR t.table_name = DECODE(r.sqltablename,' ','PSY'||r.recname,r.sqltablename)
         OR t.table_name = DECODE(r.sqltablename,' ','GFC_'||r.recname,r.sqltablename))
 ) LOOP
  set_table_prefs(i.table_name);
  IF i.rectype = 7 THEN
   FOR j IN (
    SELECT t.table_name
    FROM   pstemptblcntvw c
    ,      user_tables t
    ,      psoptions o
    ,      (SELECT rownum n FROM DUAL CONNECT BY LEVEL <= 99) n
    WHERE  c.recname = i.recname
    AND    n.n <= c.temptblinstances+o.temptblinstances
    AND    t.table_name = i.table_name||n.n
   ) LOOP
    set_table_prefs(j.table_name);
   END LOOP;
  END IF;
 END LOOP;
END set_record_prefs;
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
--public procedure called from DDL model for %UpdateStats
--12.2.2009 force stats collection on regular tables, skip GTTs
------------------------------------------------------------------------------------------------
PROCEDURE ps_stats
(p_ownname      IN VARCHAR2
,p_tabname      IN VARCHAR2
,p_verbose      IN BOOLEAN DEFAULT FALSE /*if true print SQL*/
) IS
 l_temporary        VARCHAR2(1 CHAR);
 l_partitioned      VARCHAR2(1 CHAR);
 l_recname          VARCHAR2(15 CHAR);
 l_rectype          INTEGER;
 l_temptblinstance  VARCHAR2(2 CHAR) := '';
 l_msg              VARCHAR2(200 CHAR);
 l_tablen           INTEGER;
 l_gather_stats     VARCHAR2(1);
 l_force            BOOLEAN := FALSE;
BEGIN
 msg('ps_stats(ownname=>'||p_ownname||',tabname=>'||p_tabname||')');
 l_tablen := LENGTH(p_tabname);
 l_msg := p_ownname||'.'||p_tabname||': ';

 BEGIN --is this a GTT or a partitioned table? y/n
  SELECT temporary, SUBSTR(partitioned,1,1)
  INTO   l_temporary, l_partitioned
  FROM   all_tables
  WHERE  owner = UPPER(p_ownname) /*30.3.2017 added upper()*/
  AND    table_name = UPPER(p_tabname); /*30.3.2017 added upper()*/

  IF l_temporary = 'Y' THEN
   l_msg := l_msg||'GTT. ';
  END IF;

  IF l_partitioned = 'Y' THEN
   l_msg := l_msg||'Partitioned Table. ';
  END IF;

 EXCEPTION WHEN no_data_found THEN
  RAISE_APPLICATION_ERROR(-20001,'Table '||p_ownname||'.'||p_tabname||' does not exist');
 END;

 table_to_recname(p_tabname, l_recname, l_rectype, l_temptblinstance, l_msg);

 --to introduce 'per record' behaviour per program use list of programs here
 IF 1=1 /*psftapi.get_prcsname() IN(<program name list>)*/ THEN
  BEGIN --get override meta data
   SELECT o.gather_stats
   INTO   l_gather_stats
   FROM   ps_gfc_stats_ovrd o
   WHERE  recname = l_recname;

   l_msg := l_msg||'Meta Data: '||l_gather_stats;
   l_msg := l_msg||'. ';
   l_force := TRUE;

  EXCEPTION 
   WHEN no_data_found THEN 
    l_force := TRUE; --17.11.2011 Default is to collect stats if no meta data even if table locked
    l_msg := l_msg||'No Meta Data Found. ';
  
    IF l_rectype = 0 THEN 
     l_msg := l_msg||'SQL Table. ';
     l_gather_stats := 'G'; --3.4.2014 changed default from refresh stale on normal tables to gather
    ELSIF l_rectype = 7 THEN --1.10.2009 changed default from N (No Stats) to G (Gather Stats) on temp records
     l_msg := l_msg||'Temporary Table. ';
     l_gather_stats := 'G'; --default gather stats on temp records
    END IF;
  END;
 ELSE
  l_msg := l_msg||'Default ';
  l_gather_stats := 'G';
 END IF;
--msg('Gather Stats='''||l_gather_stats||'''');
--msg('Temporary   ='''||l_temporary||'''');
--msg('Temp Inst   ='''||l_temptblinstance||'''');

 IF l_gather_stats = 'N' THEN -- do not collect stats if meta data says N

  l_msg := l_msg||'Statistics Not Collected. ';
  IF p_verbose THEN
   msg(l_msg,TRUE);
  END IF;

 ELSIF l_gather_stats = 'D' THEN --delete stats
  l_msg := l_msg||'Statistics deleted. ';
  IF p_verbose THEN
   msg(l_msg,TRUE);
  END IF;

  dbms_stats.delete_table_stats --delete statistics on table - will cascade to indexes, columns and partitions by default
  (ownname      => p_ownname
  ,tabname      => p_tabname
  ,force        => l_force
  );
  
 ELSIF l_gather_stats != 'N' AND l_temporary = 'Y' AND l_temptblinstance IS NULL THEN -- do not collect stats on shared GTTs

  l_msg := l_msg||'Statistics not collected on shared GTT. ';
  IF p_verbose THEN
   msg(l_msg,TRUE);
  END IF;
  l_gather_stats := 'N';

 ELSIF l_partitioned = 'Y' OR l_gather_stats = 'R' THEN --refresh stale if partitioned 

  l_msg := l_msg||'Refresh Stale Statistics. ';
  IF p_verbose THEN
   msg(l_msg,TRUE);
  END IF;

  gfcpsstats11.refresh_stats
  (p_ownname      => p_ownname
  ,p_tabname      => p_tabname
  ,p_force        => l_force
  ,p_verbose      => p_verbose
  ); --refresh stale stats only
   
 ELSE

  l_msg := l_msg||'Gather Statistics. ';
  IF p_verbose THEN
   msg(l_msg,TRUE);
  END IF;

  --NB delete stats will cascade to index, columns and parts by default
  gfcpsstats11.gather_table_stats
  (p_ownname      => p_ownname
  ,p_tabname      => p_tabname
  ,p_cascade      => TRUE
  ,p_force        => l_force
  ,p_verbose      => p_verbose
  ); 

  IF p_verbose THEN
   msg('Table('||p_ownname||'.'||p_tabname||' finished'||')'); 
  END IF;
 END IF;
EXCEPTION
 WHEN table_stats_locked THEN NULL;
END ps_stats;

------------------------------------------------------------------------------------------------
--procedure to create metadata from actual table preferences
------------------------------------------------------------------------------------------------
--cascade
PROCEDURE generate_metadata IS
 l_recname          VARCHAR2(15);
 l_rectype          INTEGER;
 l_temptblinstance  VARCHAR2(2 CHAR) := '';
 l_msg              VARCHAR2(200 CHAR);
 l_lock_stats       VARCHAR2(1 CHAR);
BEGIN
 FOR i IN (
  SELECT *
  FROM   user_tab_stat_prefs
  PIVOT ( 
    MAX(preference_value) FOR preference_name IN
    ('ESTIMATE_PERCENT' estimate_percent
	,'METHOD_OPT' method_opt
    ,'DEGREE' degree
    ,'GRANULARITY' granularity
    ,'INCREMENTAL' incremental
    ,'STALE_PERCENT' stale_percent
    ,'APPROXIMATE_NDV_ALGORITHM' approx_ndv
    ,'PREFERENCE_OVERRIDES_PARAMETER' pref_over_param
   ))
  ORDER BY table_name
 ) LOOP
  table_to_recname
  (p_tabname         =>i.table_name
  ,p_recname         =>l_recname
  ,p_rectype         =>l_rectype
  ,p_temptblinstance =>l_temptblinstance
  ,p_msg             =>l_msg
  );

  IF l_recname IS NOT NULL THEN  
    BEGIN
      SELECT DECODE(stattype_locked,'ALL','Y','N')
      INTO   l_lock_stats
      FROM   user_tab_statistics 
      WHERE  table_name = i.table_name
      AND    partition_name IS NULL
      AND    subpartition_name IS NULL;
    EXCEPTION WHEN no_data_found THEN
      l_lock_stats := '';
    END;
  
    IF (l_rectype = 0 AND l_lock_stats = 'N') OR (l_rectype = 7 AND l_lock_stats = 'Y')
      THEN 
        l_lock_stats := '';
    END IF;

    BEGIN
      INSERT INTO ps_gfc_stats_ovrd
      (recname, gather_stats, block_sample, estimate_percent, method_opt, degree, granularity, incremental, stale_percent, approx_ndv, pref_over_param, lock_stats)
      VALUES
      (l_recname, 'G', ' '
      ,NVL(i.estimate_percent,' ')
      ,NVL(i.method_opt,' ')
      ,NVL(i.degree,' ')
      ,NVL(i.granularity,' ')
      ,NVL(i.incremental,' ')
      ,NVL(i.stale_percent,0)
      ,NVL(i.approx_ndv,' ')
      ,NVL(i.pref_over_param,' ')
      ,NVL(l_lock_stats,' ')
      );
    EXCEPTION WHEN dup_val_on_index THEN
      UPDATE ps_gfc_stats_ovrd
      SET    estimate_percent = NVL(i.estimate_percent,' ')
      ,      method_opt = NVL(i.method_opt,' ')
      ,      degree = NVL(i.degree,' ')
      ,      granularity = NVL(i.granularity,' ')
      ,      incremental = NVL(i.incremental,' ')
      ,      stale_percent = NVL(i.stale_percent,0)
      ,      approx_ndv = NVL(i.approx_ndv,' ')
      ,      pref_over_param = NVL(i.pref_over_param,' ')
      ,      lock_stats = NVL(l_lock_stats,' ')
      WHERE  recname = l_recname;
    END;
  END IF;
  
  --merge in lock information for locked type 0 records, or records with current lock overrides
  MERGE INTO ps_gfc_stats_ovrd u 
  USING (
  SELECT r.recname, s.table_name, DECODE(s.stattype_locked,'ALL','Y') lock_stats
  FROM   user_tab_statistics s
  ,      psrecdefn r
         LEFT OUTER JOIN ps_gfc_stats_ovrd o
           ON o.recname = r.recname
  WHERE  r.rectype = 0
  AND    s.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
  AND    (s.stattype_locked IS NOT NULL OR o.lock_stats IS NOT NULL)
  ) s
  ON (u.recname = s.recname)
  WHEN MATCHED THEN UPDATE
  SET u.lock_stats = s.lock_stats
  WHEN NOT MATCHED THEN INSERT
  (recname, gather_stats, block_sample, estimate_percent, method_opt, degree, granularity, incremental, stale_percent, approx_ndv, pref_over_param, lock_stats)
  VALUES
  (s.recname, 'G', ' ', ' ', ' ', ' ', ' ', ' ', 0, ' ', ' ', s.lock_stats);
  
 END LOOP;
END generate_metadata;
------------------------------------------------------------------------------------------------
END gfcpsstats11;
/
show errors

------------------------------------------------------------------------------------------------
--trigger to set preferences on table when metadata is changed
--using dbms_job because it doesn't commit so jobs only submitted when trigger commits and 
--packages can read the table safely
------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER gfc_stats_ovrd_metadata
AFTER INSERT OR UPDATE OR DELETE ON ps_gfc_stats_ovrd
FOR EACH ROW
DECLARE
  l_cmd VARCHAR2(100) := '';
  l_jobno NUMBER;
BEGIN
  IF DELETING THEN
    dbms_job.submit(l_jobno,'gfcpsstats11.set_record_prefs('''||:old.recname||''');');
  ELSE
    IF :new.recname != :old.recname THEN
      dbms_job.submit(l_jobno,'gfcpsstats11.set_record_prefs('''||:old.recname||''');');
    END IF;
    dbms_job.submit(l_jobno,'gfcpsstats11.set_record_prefs('''||:new.recname||''');');
  END IF;
END gfc_stats_ovrd_metadata;
/
show errors

------------------------------------------------------------------------------------------------
--trigger to set preferences on table as it is created
------------------------------------------------------------------------------------------------
DROP TRIGGER gfc_locktemprecstats;
CREATE OR REPLACE TRIGGER gfc_stats_ovrd_create_table
AFTER CREATE ON sysadm.schema
DECLARE
  l_jobno NUMBER;
  e_object_already_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_object_already_exists,-27477);
BEGIN
  IF ora_dict_obj_type = 'TABLE' THEN
    --submit one-time job to set table preferences as table will not have been created by time trigger runs
    BEGIN
      sys.dbms_scheduler.create_job
      (job_name   => SUBSTR('STAT_PREFS_'||ora_dict_obj_name,1,30)
      ,job_type   => 'PLSQL_BLOCK'
      ,job_action => 'BEGIN gfcpsstats11.set_table_prefs(p_tabname=>'''||ora_dict_obj_name||'''); END;'
      ,start_date => SYSTIMESTAMP --run job immediately
      ,enabled    => TRUE --job is enabled
      ,auto_drop  => TRUE --request will be dropped when complete
      ,comments   => 'Set table preferences on table '||ora_dict_obj_owner||'.'||ora_dict_obj_name
      );
    EXCEPTION WHEN e_object_already_exists THEN NULL; --suppress duplicate object error that occurs if job already there
    END;
  END IF;
END;
/
show errors

------------------------------------------------------------------------------------------------
--trigger change COBOL stored statements that call DBMS_STATS to call this pacakge
------------------------------------------------------------------------------------------------
--25.04.2014 replace double semi-colon on end of stored statement with single semi-colon
------------------------------------------------------------------------------------------------
DROP TRIGGER gfc_stat_ovrd_stored_stmt;
--CREATE OR REPLACE TRIGGER gfc_stat_ovrd_stored_stmt
--BEFORE INSERT ON ps_sqlstmt_tbl
--FOR EACH ROW
--WHEN (new.stmt_text LIKE '%UPDATESTATS(%)')
--DECLARE
--  l_jobno NUMBER;
--BEGIN
--  :new.stmt_text := 'BEGIN gfcpsstats11.ps_stats(p_ownname=>user,p_tabname=>'''
--                     ||SUBSTR(:new.stmt_text,14,LENGTH(:new.stmt_text)-14)||'''); END;';
--END;
--/

--25.04.2014 update statement corrects any stored statements incorrectly updated by previous version of trigger 
--UPDATE 	ps_sqlstmt_Tbl
--SET 	stmt_text = REPLACE(stmt_text,'END;;','END;')
--WHERE 	stmt_text like 'BEGIN%gfcpsstats11.ps_stats(%); END;;'
--/
--COMMIT
--/


show errors
spool off
