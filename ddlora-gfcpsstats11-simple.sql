REM ddlora-gfcpsstats11-simple.sql
REM (c) Go-Faster Consultancy 2008-21
REM https://www.psftdba.com/gfcpsstats11.pdf
spool ddlora-gfcpsstats11-simple

UPDATE PSDDLMODEL 
SET    MODEL_STATEMENT = 'DBMS_STATS.GATHER_TABLE_STATS (ownname=>[DBNAME], tabname=>[TBNAME], force=>TRUE);' 
WHERE  PLATFORMID=2 
AND    STATEMENT_TYPE IN (4,5) 
;

spool off
