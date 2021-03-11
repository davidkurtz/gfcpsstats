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
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, pref_over_param, lock_stats)
SELECT 	recname,'G',' ',' ',' ',' ',' ',' ',0,' ','Y'
from psrecdefn a
where rectype = 0
and (recname like 'PSTREESELECT__'
or recname like 'JRNL_HDR_TMP%'
or recname like 'CFV_SEL%'
or recname like 'CLO_ABG_TMP'
or recname like 'CURR_WRK_RT'
or recname like 'CLO_ACCT_TMP%'
or recname like 'CLO_LEDG_TMP%'
or recname like 'CLO_EARN_TMP%'
or recname like 'CURR_SEL10_R%'
or recname like 'TREE_SEL10_R%')
AND NOT EXISTS(
	SELECT 'x'
	FROM	ps_gfc_stats_ovrd b
 	WHERE 	b.recname = a.recname);

commit;

--EXECUTE gfcpsstats11.generate_metadata;
------------------------------------------------------------------------------------------
set serveroutput on pages 40 wrap on trimspool on 
clear screen
column recname          format a15 heading 'PS Record Name'
column gather_stats     format a6  heading 'Gather|Stats'
column estimate_percent format a8  heading 'Estimate|Percent'
column block_sample     format a6  heading 'Block|Sample'
column method_opt       format a30 
column degree           format a10
column granularity      format a10
column incremental      format a10
column stale_percent    heading 'Stale|Percent' format 999
column pref_over_param  heading 'Pref|Over|Param' format a5
column lock_stats       format a5 heading 'Lock|Stats'
SELECT * 
FROM   ps_gfc_stats_ovrd 
ORDER BY recname
/
------------------------------------------------------------------------------------------
column table_name       format a18
column preference_name  format a16
column preference_value format a40
break on table_name 
SELECT 	* 
FROM	user_tab_stat_prefs 
order by table_name
/
------------------------------------------------------------------------------------------
spool off

------------------------------------------------------------------------------------------
