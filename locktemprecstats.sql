REM locktemprecstats.sql
REM (c)David Kurtz, Go-Faster Consultancy 2009-21
REM Delete and lock statistics on working storage tables
REM 11.03.2021 added exception processes to locking 

set pages 99 lines 100 timi on autotrace off trimspool on
column recname format a15
column rectype heading 'Rec|Type' format 99
column table_name format a18
column lock_stats heading 'Lock|Stats' format a5
column num_rows heading 'Num|Rows'
column stattype_locked heading 'Stattype|Locked' format a8
ttitle 'Unlocked Temporary Tables'
spool locktemprecstats app
SELECT /*+LEADING(g r)*/ DISTINCT r.recname, r.rectype, t.table_name, t.last_analyzed, t.num_rows, s.stattype_locked, g.lock_stats
,      s.stattype_locked
FROM   psrecdefn r
,      ps_gfc_stats_ovrd g
,      user_tables t
         LEFT OUTER JOIN user_tab_statistics s
         ON  s.table_name = t.table_name
         AND s.partition_name IS NULL
WHERE  r.rectype = 0
AND    g.recname = r.recname
AND    t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
AND    t.temporary = 'N'
AND    ((g.lock_stats = 'Y' AND s.stattype_locked IS NULL) --stats not locked
       OR (g.lock_stats = 'N' AND s.stattype_locked IS NOT NULL))
UNION 
SELECT /*+LEADING(o i r)*/ DISTINCT r.recname, r.rectype, t.table_name, t.last_analyzed, t.num_rows, s.stattype_locked, g.lock_stats
,      s.stattype_locked
FROM   psrecdefn r
         LEFT OUTER JOIN ps_gfc_stats_ovrd g
         ON g.recname = r.recname
,      pstemptblcntvw i
,      psoptions o
,      user_tables t	
	 LEFT OUTER JOIN user_tab_statistics s
	 ON  s.table_name = t.table_name
         AND s.partition_name IS NULL
,      (SELECT rownum row_number FROM dual CONNECT BY LEVEL <= 100) v
WHERE  r.rectype = 7
AND    r.recname = i.recname
AND    v.row_number <= i.temptblinstances + o.temptblinstances
AND    t.table_name 
       	= DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
	||DECODE(v.row_number*r.rectype,100,'',LTRIM(TO_NUMBER(v.row_number))) 
AND    t.temporary = 'N'
AND    ((NVL(g.lock_stats,' ') IN('Y',' ') AND s.stattype_locked IS NULL) --stats not locked
       OR (g.lock_stats = 'N' AND s.stattype_locked IS NOT NULL))
ORDER BY 1,3
/

ttitle off


set serveroutput on
BEGIN
 FOR x IN (
  SELECT /*+LEADING(g r)*/ DISTINCT r.recname, t.table_name, t.last_analyzed, t.num_rows, NVL(g.lock_stats,'N') lock_stats
  ,      s.stattype_locked
  FROM   psrecdefn r
  ,      ps_gfc_stats_ovrd g
  ,      user_tables t
           LEFT OUTER JOIN user_tab_statistics s
           ON  s.table_name = t.table_name
           AND s.partition_name IS NULL
  WHERE  r.rectype = 0
  AND    g.recname = r.recname
  AND    t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
  AND    t.temporary = 'N'
  AND    ((g.lock_stats = 'Y' AND s.stattype_locked IS NULL) --stats not locked
         OR (g.lock_stats = 'N' AND s.stattype_locked IS NOT NULL))
  UNION 
  SELECT /*+LEADING(o i r)*/ r.recname, t.table_name, t.last_analyzed, t.num_rows, NVL(g.lock_stats,'Y') lock_stats
  ,      s.stattype_locked
  FROM   psrecdefn r
           LEFT OUTER JOIN ps_gfc_stats_ovrd g
           ON g.recname = r.recname
  ,    	pstemptblcntvw i
  ,     psoptions o
  ,     user_tables t	
	  LEFT OUTER JOIN user_tab_statistics s
	  ON  s.table_name = t.table_name
          AND s.partition_name IS NULL
  ,     (SELECT rownum row_number FROM dual CONNECT BY LEVEL <= 100) v
  WHERE r.rectype = '7'
  AND   r.recname = i.recname
  AND   t.table_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
                     ||DECODE(v.row_number*r.rectype,100,'',LTRIM(TO_NUMBER(v.row_number))) 
  AND   t.temporary = 'N'
/*---------------------------------------------------------------------            
--AND    r.recname IN('TL_PMTCH1_TMP' --TL_TA000600.SLCTPNCH.STATS1.S�
--                   ,'TL_PMTCH2_TMP' --TL_TA000600.CALC_DUR.STATS1.S�)
-----------------------------------------------------------------------*/
  AND    ((NVL(g.lock_stats,' ') IN('Y',' ') AND s.stattype_locked IS NULL) --stats not locked
         OR (g.lock_stats = 'N' AND s.stattype_locked IS NOT NULL))
  ) LOOP
    IF x.lock_stats = 'N' THEN
      dbms_output.put_line('Unlocking Statistics on '||user||'.'||x.table_name); 
      dbms_stats.unlock_table_stats(ownname=>user,tabname=>x.table_name);
    ELSE
      IF x.last_analyzed IS NOT NULL THEN --delete stats
        dbms_output.put_line('Deleting Statistics on '||user||'.'||x.table_name);
        dbms_stats.delete_table_stats(ownname=>user,tabname=>x.table_name,force=>TRUE);
      END IF;
      IF x.stattype_locked IS NULL THEN --lock stats
        dbms_output.put_line('Locking Statistics on '||user||'.'||x.table_name); 
        dbms_stats.lock_table_stats(ownname=>user,tabname=>x.table_name);
      END IF;
    END IF;
  END LOOP;
END;
/


spool off
