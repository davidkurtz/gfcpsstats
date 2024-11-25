REM ddlora-gfcpsstats11.sql
REM (c) Go-Faster Consultancy 2008-21
REM https://www.psftdba.com/gfcpsstats11.pdf
spool ddlora-gfcpsstats11

UPDATE PSDDLMODEL
SET    MODEL_STATEMENT = 'gfcpsstats11.ps_stats(p_ownname=>[DBNAME],p_tabname=>[TBNAME]);'
WHERE  PLATFORMID=2
AND    STATEMENT_TYPE IN (4,5)
;

spool off
