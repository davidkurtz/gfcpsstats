REM gfcpsstats11_testdata.sql
REM (c) Go-Faster Consultancy 2008-2012
/*------------------------------------------------------------------------------------------
/* the following commands are to test the stats with gfcpsstats11
/*------------------------------------------------------------------------------------------

spool gfcpsstats11_testdata
set serveroutput on

column table_name format a20
column preference_name format a20
column preference_value format a40

ALTER SESSION SET NLS_DATE_FORMAT='hh24:mi:ss dd.mm.yyyy';
clear screen
DELETE FROM ps_gfc_stats_ovrd;
commit;

BEGIN
 FOR i IN (SELECT * FROM user_tab_stat_prefs) LOOP
   sys.dbms_stats.delete_table_prefs(ownname=>user, tabname=>i.table_name, pname=>i.preference_name);
 END LOOP;
END;
/

SELECT 	*
FROM	user_tab_stat_prefs
/

DELETE FROM ps_gfc_stats_ovrd;
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES ('TL_PROF_WRK','D','0',' ',' ',' ',' ',' ',0);
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES ('TL_PROF_LIST','G','42',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0);

commit
/
SELECT 	*
FROM	user_tab_stat_prefs
WHERE   table_name = 'PS_TL_PROF_LIST62'
order by table_name
/
update ps_gfc_stats_ovrd set estimate_percent = 'DBMS_STATS.AUTO_SAMPLE_SIZE'
/
commit
/
SELECT 	*
FROM	user_tab_stat_prefs
WHERE   table_name = 'PS_TL_PROF_LIST62'
order by table_name
/
update ps_gfc_stats_ovrd set estimate_percent = '4'
/
commit
/
SELECT 	*
FROM	user_tab_stat_prefs
WHERE   table_name = 'PS_TL_PROF_LIST62'
order by table_name
/

--these collect stats iff stale
begin gfcpsstats11.ps_stats('SYSADM','PSLOCK', TRUE); end;
/
begin gfcpsstats11.ps_stats('SYSADM','PSLOCK', TRUE); end;
/
begin gfcpsstats11.ps_stats('SYSADM','PSLOCK', TRUE); end;
/

--expect collect stats at 42%
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_PROF_LIST12',TRUE); end;
/
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_PROF_LIST6',TRUE); end;
/
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_PROF_LIST',TRUE); end;
/

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES ('TL_WRK01_RCD','N',' ',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0);

--will not collect stats
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_WRK01_RCD',TRUE); end;
/

select table_name, num_rows, last_analyzed from user_tables
where table_name = 'PS_TL_ST_PCHTIME'
or table_name = 'PS_TL_PROF_LIST12'
/
--no message but it does collect
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_PROF_LIST12'); end;
/
--no message but it does collect iff stale
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_ST_PCHTIME'); end;
/
select table_name, num_rows, last_analyzed from user_tables
where table_name = 'PS_TL_ST_PCHTIME'
or table_name = 'PS_TL_PROF_LIST12'
/

--check it deletes stats
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_PROF_WRK12',TRUE); end;
/

--check DDL trigger works
DELETE FROM ps_gfc_stats_ovrd;
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES ('TL_PROF_LIST','G','42',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0);

SELECT 	*
FROM	user_tab_stat_prefs
WHERE   table_name = 'PS_TL_PROF_LIST62'
order by table_name
/

DROP TABLE PS_TL_PROF_LIST62
/
SELECT 	*
FROM	user_tab_stat_prefs
WHERE   table_name = 'PS_TL_PROF_LIST62'
order by table_name
/

CREATE TABLE PS_TL_PROF_LIST62 (PROCESS_INSTANCE DECIMAL(10) NOT NULL,
   EMPLID VARCHAR2(11) NOT NULL,
   EMPL_RCD SMALLINT NOT NULL,
   START_DT DATE,
   END_DT DATE) TABLESPACE TLWORK STORAGE (INITIAL 40000 NEXT 100000
 MAXEXTENTS UNLIMITED PCTINCREASE 0) PCTFREE 10 PCTUSED 80
/
CREATE UNIQUE  iNDEX PS_TL_PROF_LIST62 ON PS_TL_PROF_LIST62
 (PROCESS_INSTANCE,
   EMPLID,
   EMPL_RCD) TABLESPACE PSINDEX STORAGE (INITIAL 40000 NEXT 100000
 MAXEXTENTS UNLIMITED PCTINCREASE 0) PCTFREE 10 PARALLEL NOLOGGING
/
ALTER INDEX PS_TL_PROF_LIST62 NOPARALLEL LOGGING
/

SELECT 	*
FROM	user_tab_stat_prefs
WHERE   table_name = 'PS_TL_PROF_LIST62'
order by table_name
/





--now lets set up for a n --added 6.6.2009
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES('TL_TA_BATCHA','G',' ',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0); 
execute psftapi.set_prcsinstance(4,'GFCTEST');

DELETE FROM ps_aetemptblmgr
WHERE process_instance = 0
/
INSERT INTO ps_aetemptblmgr 
(process_instance, recname, curtempinstance, oprid, run_cntl_id, ae_applid, run_dttm, ae_disable_restart, ae_dedicated, ae_truncated)
VALUES (0,'TL_TA_BATCHA',4,'PS','Wibble','GFCTEST',SYSDATE,'N',1,1) --restart enabled
/
--this will collect because it is permanent
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_TA_BATCHA4',TRUE); end;
/
DELETE FROM ps_gfc_stats_ovrd WHERE recname = 'TL_TA_BATCHA';
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_TA_BATCHA',TRUE); end;
/


DELETE FROM ps_aetemptblmgr
WHERE process_instance = 0
/
INSERT INTO ps_aetemptblmgr
(process_instance, recname, curtempinstance, oprid, run_cntl_id, ae_applid, run_dttm, ae_disable_restart, ae_dedicated, ae_truncated)
VALUES (0,'TL_TA_BATCHA',4,'PS','Wibble','GFCTEST',SYSDATE,'Y',1,1) --restart disabled
/

--and this will collect stats on non-shared instance of GTT.
INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES('TL_TA_BATCHA','G',' ',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0); 
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_TA_BATCHA4',TRUE); end;
/
DELETE FROM ps_gfc_stats_ovrd WHERE recname = 'TL_TA_BATCHA';
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_TA_BATCHA4',TRUE); end;
/

INSERT INTO ps_gfc_stats_ovrd (recname, gather_stats, estimate_percent, block_sample, method_opt, degree, granularity, incremental, stale_percent) 
VALUES('TL_TA_BATCHA','G',' ',' ','FOR ALL COLUMNS SIZE 1',' ',' ',' ',0); 
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_TA_BATCHA',TRUE); end;
/
DELETE FROM ps_gfc_stats_ovrd WHERE recname = 'TL_TA_BATCHA';
begin gfcpsstats11.ps_stats('SYSADM','PS_TL_TA_BATCHA',TRUE); end;
/

DELETE FROM ps_gfc_stats_ovrd 
/


--now lets try GTTs
begin gfcpsstats11.ps_stats('SYSADM','PS_GP_PYE_STAT_WRK',TRUE); end;
/
begin gfcpsstats11.ps_stats('SYSADM','PS_GP_GL_DATATMP',TRUE); end;
/
begin gfcpsstats11.ps_stats('SYSADM','PS_GP_GL_DATATMP12',TRUE); end;
/


--demo partition refresh
set serveroutput on 
update ps_gp_rslt_acum subpartition(GP_RSLT_ACUM_018_Z_OTHERS) set user_key6 = user_key6 where rownum <= 100000;
commit;
begin gfcpsstats11.ps_stats(p_ownname=>'SYSADM', p_tabname=>'PS_GP_RSLT_ACUM', p_verbose=>TRUE); end;
/
begin gfcpsstats11.ps_stats(p_ownname=>'SYSADM', p_tabname=>'PS_GP_RSLT_ACUM', p_verbose=>TRUE); end;
/


--loop to delete all preferneces for this user
BEGIN
 FOR i IN (SELECT * FROM user_tab_stat_prefs) LOOP
   sys.dbms_stats.delete_table_prefs(ownname=>user, tabname=>i.table_name, pname=>i.preference_name);
 END LOOP;
END;
/

pause
------------------------------------------------------------------------------------------*/