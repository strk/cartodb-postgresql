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
SELECT CDB_DropForeignTable('fdw_spatial_ref_sys');

-- Valid table with username
select cdb_createforeigntable(
  'postgresql://' || current_user || '@localhost/' || current_database(),
  'spatial_ref_sys'
);
SELECT * FROM spatial_ref_sys EXCEPT SELECT * FROM fdw_spatial_ref_sys;
SELECT CDB_DropForeignTable('fdw_spatial_ref_sys');

-- Valid pgsql table with geometry column
CREATE TABLE points AS SELECT CDB_LatLng(0,0) as g, 1::int as i, NOW() as d;
select cdb_createforeigntable(
  'postgresql://localhost/' || current_database(),
  'points'
);
SELECT i, ST_AsEWKT(g), floor(extract(secs from NOW()-d)) from fdw_points;
SELECT CDB_DropForeignTable('fdw_points');
DROP TABLE points;

-- Valid mysql table with geometry column
-- (disabled as I can't control presence of a mysql server)
--select cdb_createforeigntable('mysql://localhost/test','geomtest');
--SELECT CDB_DropForeignTable('fdw_geomtest');

