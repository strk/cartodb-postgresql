\i scripts-available/CDB_CreateForeignTable.sql
\set VERBOSITY terse
-- Invalid DSN
select cdb_createforeigntable('xxx://','x');
-- Missing DSN parts
select cdb_createforeigntable('postgresql://','x');
-- Connection refused
select cdb_createforeigntable('postgresql://localhost:1111','x');
-- Table not found
select cdb_createforeigntable('postgresql://localhost','notfound');
-- Valid table
select cdb_createforeigntable('postgresql://localhost','spatial_ref_sys');
-- Valid pgsql table with geometry column
select cdb_createforeigntable('postgresql://localhost','lots_of_points');
-- Valid mysql table with geometry column
select cdb_createforeigntable('mysql://localhost/test','geomtest');
