\set VERBOSITY terse
-- Invalid DSN
select cdb_createforeigntable('xxx://','x');
-- Missing DSN parts
select cdb_createforeigntable('postgresql://','x');
-- Connection refused
select cdb_createforeigntable(
  'postgresql://localhost:1111/' || current_database(),
  'x'
);
-- Table not found
select cdb_createforeigntable(
  'postgresql://localhost/' || current_database(),
  'notfound'
);
-- Valid table
select cdb_createforeigntable(
  'postgresql://localhost/' || current_database(),
  'spatial_ref_sys'
);

SELECT CDB_DropForeignTable(
  'cdb_fdwsrv_postgresql_localhost_strk_spatial_ref_sys'
);

-- Valid table with username
select cdb_createforeigntable(
  'postgresql://' || current_user || '@localhost/' || current_database(),
  'spatial_ref_sys'
);
-- Valid pgsql table with geometry column
select cdb_createforeigntable(
  'postgresql://localhost/' || current_database(),
  'lots_of_points'
);
-- Valid mysql table with geometry column
select cdb_createforeigntable('mysql://localhost/test','geomtest');

-- TODO: use CDB_DropForeignTable ?
DROP SERVER cdb_fdwsrv_postgresql_localhost_strk_spatial_ref_sys CASCADE;
DROP SERVER cdb_fdwsrv_postgresql_localhost_strk_lots_of_points CASCADE;
DROP SERVER cdb_fdwsrv_mysql_localhost_test_geomtest CASCADE;
