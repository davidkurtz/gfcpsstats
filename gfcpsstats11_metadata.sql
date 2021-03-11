REM gfcpsstats11_metadata.sql
REM (c) Go-Faster Consultancy 2008-2012
------------------------------------------------------------------------------------------
--this script containts sample meta data table designed to minimize stats collection in TL 
--processing these tables need statistics, but not histograms.  Optimizer Dynamic Sample 
--is sufficient for the other temporary records used by TL_TIMEADMIN
--
--this list is just a suggestion - it worked at on one system - YOUR MILEAGE MAY VARY
--
--25.04.2014: On 11g you might be able to disable stats collection on TL_WRK01_RCD, but you still need stats on TL_PROF_WRK
------------------------------------------------------------------------------------------
clear screen
set echo on serveroutput on lines 100 wrap off
spool gfcpsstats11_metadata

REM DELETE FROM ps_gfc_stats_ovrd;

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, pref_over_param, lock_stats)
SELECT 	recname,'G',' ',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0,' ',' '
FROM    psrecdefn a
WHERE   rectype IN(0,7)
AND     recname IN ('TL_IPT1'       ,'TL_MTCHD'     ,'TL_PMTCH1_TMP' ,'TL_PMTCH2_TMP' ,'TL_PMTCH_TMP1' 
                   ,'TL_PMTCH_TMP2' ,'TL_PROF_LIST' ,'TL_PROF_WRK'   ,'TL_RESEQ2_WRK' ,'TL_RESEQ5_WRK'
                   ,'TL_WRK01_RCD'  ,'WRK_SCHRS_TAO')
AND NOT EXISTS(
	SELECT 'x'
	FROM	ps_gfc_stats_ovrd b
 	WHERE 	b.recname = a.recname);


INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, pref_over_param, lock_stats)
SELECT 	recname,'R',' ',' ',' ',' ',' ',' ',0,' ',' '
FROM    psrecdefn a
WHERE   rectype IN(0,7)
AND     recname IN('TL_FRCS_PYBL_TM', 'TL_ST_PCHTIME', 'TL_VALID_TR')
AND NOT EXISTS(
	SELECT 'x'
	FROM	ps_gfc_stats_ovrd b
 	WHERE 	b.recname = a.recname)
;

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent, pref_over_param, lock_stats)
SELECT 	DISTINCT recname,'N',' ',' ',' ',' ',' ',' ',0,' ',' '
FROM	PSAEAPPLTEMPTBL a
WHERE 	a.ae_applid = 'TL_TIMEADMIN'
AND NOT EXISTS(
	SELECT 'x'
	FROM	ps_gfc_stats_ovrd b
 	WHERE 	b.recname = a.recname);

commit;

--EXECUTE gfcpsstats11.generate_metadata;
------------------------------------------------------------------------------------------
set serveroutput on pages 40
clear screen
column recname          format a15 heading 'PS Record Name'
column method_opt       format a60
column gather_stats     format a6  heading 'Gather|Stats'
column estimate_percent format a8  heading 'Estimate|Percent'
column block_sample     format a6  heading 'Block|Sample'
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
