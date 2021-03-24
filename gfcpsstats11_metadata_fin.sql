REM gfcpsstats11_metadata_fin.sql
REM (c) Go-Faster Consultancy 2008-2021
------------------------------------------------------------------------------------------
--this script containts sample meta data table designed to minimize stats collection in Financials
--
--this list is just a suggestion - it worked at on one system - YOUR MILEAGE MAY VARY
------------------------------------------------------------------------------------------
clear screen
set echo on serveroutput on lines 180 wrap off trimspool on
spool gfcpsstats11_metadata_fin

REM DELETE FROM ps_gfc_stats_ovrd;

--permanent tables used for temporary working storage where we are going to lock and delete statistics
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, approx_ndv, pref_over_param, lock_stats)
SELECT 	recname,'G',' ',' ',' ',' ',' ',' ',0,' ',' ','Y' 
from psrecdefn a
where rectype = 0
and (recname like 'PSTREESELECT__'
or recname like 'JRNL_HDR_TMP%'
or recname like 'CFV_SEL%'
or recname like 'CURR_WRK_RT' 
or recname like 'CLO_ABG_TMP' 
or recname like 'CLO_ACCT_TMP%' /*GLPCLOSE*/ 
or recname like 'CLO_ENBD_TMP%'
or recname like 'CLO_LGBD_TMP%'
or recname like 'CLO_EARN_TMP%'  
or recname like 'CLO_JLN2_TMP%' /*GLPCLOSE*/
or recname like 'CLO_JNHD_TMP%' /*GLPCLOSE*/
or recname like 'CLO_JNLN_TMP%' /*GLPCLOSE*/
or recname like 'CLO_LEDG_TMP%' /*GLPCLOSE*/
or recname like 'CURR_SEL10_R%' 
or recname like 'TREE_SEL10_R%' 
) AND NOT EXISTS(
  SELECT 'x'
  FROM   ps_gfc_stats_ovrd b 
  WHERE  b.recname = a.recname) 
;

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, approx_ndv, pref_over_param, lock_stats) 
SELECT recname,'G',' ',' ','FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 1 RATE_MULT RATE_DIV ',' ',' ',' ',1,' ',' ','N' 
from   psrecdefn a 
where  a.recname = 'RT_RATE_TBL';

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, approx_ndv, pref_over_param, lock_stats) 
SELECT recname,'G',' ',' ','FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 (SETID, PROCESS_GROUP) FOR COLUMNS SIZE 1 RANGE_FROM_'||SUBSTR(recname,-2)||' RANGE_TO_'||SUBSTR(recname,-2)
,      ' ',' ',' ',0,' ',' ','N' 
from   psrecdefn a 
where  a.recname like 'COMBO_SEL___';

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, approx_ndv, pref_over_param, lock_stats) 
SELECT recname,'G',' ',' ','FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 (SETID, PROCESS_GROUP) FOR COLUMNS SIZE 1 RANGE_FROM_30'
,      '5',' ',' ',0,' ',' ','N' 
from   psrecdefn a 
where  a.recname like 'COMB_EXPLODED';

--stale=1
MERGE INTO ps_gfc_stats_ovrd u USING (
SELECT recname
,      1 stale_percent 
from   psrecdefn a 
where  a.rectype = 0
and    a.recname IN('RT_RATE_TBL','PSTREELEAF') 
) s ON (u.recname = s.recname) 
WHEN NOT MATCHED THEN INSERT
(u.recname, u.gather_stats, u.estimate_percent, u.block_sample, u.method_opt, u.degree, u.granularity, u.incremental, u.stale_percent, u.approx_ndv, u.pref_over_param, u.lock_stats) 
VALUES
(s.recname, 'G', ' ', ' ', ' ', ' ', ' ', ' ', s.stale_percent, ' ', ' ', ' ') 
WHEN MATCHED THEN UPDATE 
SET u.stale_percent = s.stale_percent
;

--degree=5
MERGE INTO ps_gfc_stats_ovrd u USING (
SELECT recname
,      '5' degree 
from   psrecdefn a 
where  a.rectype = 0
and    a.recname IN('PS_COMBO_SEL_10','PS_COMB_EXPLODED')
) s ON (u.recname = s.recname) 
WHEN NOT MATCHED THEN INSERT
(u.recname, u.gather_stats, u.estimate_percent, u.block_sample, u.method_opt, u.degree, u.granularity, u.incremental, u.stale_percent, u.approx_ndv, u.pref_over_param, u.lock_stats) 
VALUES
(s.recname, 'G', ' ', ' ', ' ', s.degree, ' ', ' ', 0, ' ', ' ', ' ') 
WHEN MATCHED THEN UPDATE 
SET u.degree = s.degree
;


--degree=10
MERGE INTO ps_gfc_stats_ovrd u USING (
SELECT recname
,      '10' degree 
from   psrecdefn a 
where  a.rectype = 0
and    a.recname IN('PSTREELEAF', 'PSTREENODE'
                   ,'RT_RATE_TBL') 
) s ON (u.recname = s.recname) 
WHEN NOT MATCHED THEN INSERT
(u.recname, u.gather_stats, u.estimate_percent, u.block_sample, u.method_opt, u.degree, u.granularity, u.incremental, u.stale_percent, u.approx_ndv, u.pref_over_param, u.lock_stats) 
VALUES
(s.recname, 'G', ' ', ' ', ' ', s.degree, ' ', ' ', 0, ' ', ' ', ' ') 
WHEN MATCHED THEN UPDATE 
SET u.degree = s.degree
;


--degree=20 
MERGE INTO ps_gfc_stats_ovrd u USING (
SELECT recname
,      '20' degree 
from   psrecdefn a 
where  a.rectype = 0
and    a.recname IN('LEDGER_ADB_MTD','LEDGER_ADB_QTD','LEDGER_ADB_YTD') 
) s ON (u.recname = s.recname) 
WHEN NOT MATCHED THEN INSERT
(u.recname, u.gather_stats, u.estimate_percent, u.block_sample, u.method_opt, u.degree, u.granularity, u.incremental, u.stale_percent, u.approx_ndv, u.pref_over_param, u.lock_stats) 
VALUES
(s.recname, 'G', ' ', ' ', ' ', s.degree, ' ', ' ', 0, ' ', ' ', ' ') 
WHEN MATCHED THEN UPDATE 
SET u.degree = s.degree
;

--incremental, degree 20
MERGE INTO ps_gfc_stats_ovrd u USING (
SELECT recname
,      '20' degree 
,      'TRUE' incremental 
,      5 stale_percent
,      'HYPERLOGLOG' approx_ndv 
from   psrecdefn a 
,      user_tables t 
where  a.rectype = 0 
and    t.table_name = DECODE(a.sqltablename,' ','PS_'||a.recname,a.sqltablename)
and    t.partitioned = 'YES'
and    a.recname IN('JRNL_LN','JRNL_VAT','LEDGER','LEDGER_ADB','LEDGER_BUDG') 
) s ON (u.recname = s.recname) 
WHEN NOT MATCHED THEN INSERT
(u.recname, u.gather_stats, u.estimate_percent, u.block_sample, u.method_opt, u.degree, u.granularity, u.incremental, u.stale_percent, u.approx_ndv, u.pref_over_param, u.lock_stats) 
VALUES
(s.recname, 'G', ' ', ' ', ' ', ' ', s.degree, s.incremental, s.stale_percent, s.approx_ndv, ' ', ' ') 
WHEN MATCHED THEN UPDATE 
SET u.degree = s.degree 
,   u.incremental = s.incremental 
,   u.stale_percent = s.stale_percent
,   u.approx_ndv = s.approx_ndv
,   u.granularity = ' '
;


--approx_ndv=ADAPTIVE SAMPLING
MERGE INTO ps_gfc_stats_ovrd u USING ( 
SELECT recname 
,      'HYPERLOGLOG' approx_ndv 
from   psrecdefn a 
where  a.recname IN('ALC_GL_P_TAO','ALC_GL_T_TAO') 
) s ON (u.recname = s.recname) 
WHEN NOT MATCHED THEN INSERT 
(u.recname, u.gather_stats, u.estimate_percent, u.block_sample, u.method_opt, u.degree, u.granularity, u.incremental, u.stale_percent, u.approx_ndv, u.pref_over_param, u.lock_stats) 
VALUES 
(s.recname, 'G', ' ', ' ', ' ', ' ', ' ', ' ', 0, s.approx_ndv, ' ', ' ')  
WHEN MATCHED THEN UPDATE  
SET u.approx_ndv = s.approx_ndv
;

UPDATE ps_gfc_stats_ovrd 
SET    method_opt = 'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 1 PROJECT_ID CHARTFIELD1 FB_CHRTFLD4 FB_CHRTFLD5 FB_CHRTFLD6 FB_CHRTFLD7 FB_CHRTFLD8 AFFILIATE_INTRA1 DTTM_STAMP_SEC'
WHERE  recname = 'LEDGER'
/

commit;

--EXECUTE gfcpsstats11.generate_metadata;
------------------------------------------------------------------------------------------
set serveroutput on pages 40 wrap on trimspool on 
clear screen
column recname          format a15 heading 'PS Record Name'
column gather_stats     format a6  heading 'Gather|Stats'
column estimate_percent format a8  heading 'Estimate|Percent'
column block_sample     format a6  heading 'Block|Sample'
column method_opt       format a50 
column degree           format a10
column granularity      format a10
column incremental      format a10
column stale_percent    heading 'Stale|Percent' format 999
column approx_ndv       heading 'Approximate|Number of|Distinct Values' format a20
column pref_over_param  heading 'Pref|Over|Param' format a5
column lock_stats       format a5 heading 'Lock|Stats'
SELECT * 
FROM   ps_gfc_stats_ovrd 
ORDER BY recname
/
------------------------------------------------------------------------------------------
column table_name       format a18
column preference_name  format a30
column preference_value format a80
break on table_name 
SELECT 	*  
FROM    user_tab_stat_prefs  
order by table_name, preference_name
/
------------------------------------------------------------------------------------------
spool off

------------------------------------------------------------------------------------------
