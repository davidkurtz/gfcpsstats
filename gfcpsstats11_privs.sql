REM gfcpsstats11_privs.sql
REM (c) Go-Faster Consultancy 2021

set echo on
spool gfcpsstats11_privs

GRANT EXECUTE on sys.dbms_job TO SYSADM;
GRANT CREATE JOB TO SYSADM;

spool off