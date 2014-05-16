-- Create a foreign server, user mapping and table
--
-- @param db_url an sqlalchemy connection string, like:
--               mysql://<user>:<password>@<host>/<dbname>
-- @param tablename name of the remote table
--
-- @return name of local table (as a regclass)
--
DROP FUNCTION IF EXISTS CDB_CreateForeignTable(text, text);
CREATE OR REPLACE FUNCTION CDB_CreateForeignTable(db_url text, tablename text)
RETURNS regclass AS $$

#--
#--  TODO: Rate-limit ?
#--

#--
#--  Find remote table structure
#--

from sqlalchemy import create_engine
from sqlalchemy.exc import InterfaceError, NoSuchTableError, OperationalError
from sqlalchemy.schema import Table, Column, MetaData

from urlparse import urlparse
try:
  url = urlparse(db_url)
except Exception as err:
  plpy.error(str(err))

#-- TODO: forbid anything but scheme 'mysql' or 'postgresql' ?
plpy.debug("DB_URL scheme: " + url.scheme)
if url.username:
  plpy.debug("DB_URL username: " + url.username)
if url.password:
  plpy.debug("DB_URL password: " + url.password)
if url.path:
  plpy.debug("DB_URL database: " + url.path)
else:
  plpy.error("Missing database name from " + url.geturl())

plpy.debug(url.geturl())

#---- @{ Spatial integration ----

from sqlalchemy.types import UserDefinedType
from sqlalchemy import func

# MySQL
if url.scheme == 'mysql':

  plpy.debug('Foreign server is MYSQL !')

  class Geometry(UserDefinedType):
    name = 'GEOMETRY'
    def get_col_spec(self):
      return '%s' % (self.name)
    def bind_expression(self, bindvalue):
      return func.ST_GeomFromText(bindvalue, type_=self)
    def column_expression(self, col):
      return func.AsText(col, type_=self)

  from sqlalchemy.dialects.mysql.base import ischema_names
  ischema_names['geometry'] = Geometry

  from sqlalchemy.dialects.postgresql.base import ischema_names
  ischema_names['geometry'] = Geometry

# PostgreSQL
elif url.scheme == 'postgresql':

  plpy.debug('Foreign server is PostgreSQL !')

  class Geometry(UserDefinedType):
    name = 'GEOMETRY'
    def get_col_spec(self):
      return '%s' % (self.name)

  from sqlalchemy.dialects.postgresql.base import ischema_names
  ischema_names['geometry'] = Geometry

else:

  plpy.error('Unsupported foreign server ' + url.scheme)


#---- Spatial integration @}----


try:
  metadata = MetaData(url.geturl())
except Exception as err:
  plpy.error(str(err))

try:
  tableobj = Table(tablename, metadata, autoload=True)
except InterfaceError as err:
  plpy.error(str(err))
except NoSuchTableError as err:
  plpy.error('No such table: ' + str(tablename))
except OperationalError as err:
  plpy.error( str(err).split('\n')[0] )
except Exception as err:
  plpy.error(str(err.__class__)+':'+str(err))

#--
#-- Create server (or should we reuse existing ones?)
#--

# Set server name
fdwsrvnam = 'cdb_fdwsrv_' + url.scheme + '_' + url.hostname
if url.port:
  fdwsrvnam += ':' + url.port
fdwsrvnam += '_' + url.path[1:]
fdwsrvnam += '_' + tablename
fdwsrvnam = fdwsrvnam[0:63] # truncate to pgsql limit 

plpy.debug('Server name: ' + fdwsrvnam);

# Build safe url (with no auth)
naurl = url.scheme + '://' + url.hostname
if url.port:
  naurl += ':' + url.port 
naurl += url.path

query = 'CREATE SERVER ' + plpy.quote_ident(fdwsrvnam) \
      + ' FOREIGN DATA WRAPPER multicorn OPTIONS (' \
      + ' wrapper \'multicorn.sqlalchemyfdw.SqlAlchemyFdw\'' \
      + ', db_url ' + plpy.quote_literal(naurl) \
      + ', tablename ' + plpy.quote_literal(tablename) \
      + ')'

plpy.debug('QUERY: ' + query)
plpy.execute(query)


#--
#-- Create user mapping (to hide auth)
#--

if url.username:
  query = 'CREATE USER MAPPING FOR CURRENT_USER SERVER ' \
        + plpy.quote_ident(fdwsrvnam) \
        + ' OPTIONS ( username ' + plpy.quote_literal(url.username)
  if url.password:
    query += ', password ' + plpy.quote_literal(url.password)
  query += ' )'
  plpy.debug('MAPPING QUERY: ' + query)
  plpy.execute(query)

#--
#-- Create foreign table 
#--

fdwtblnam = 'fdw_' + tablename
query = 'CREATE FOREIGN TABLE public.' + plpy.quote_ident(fdwtblnam) \
      + ' ( '
sep = ''
for c in tableobj.columns:
  typ = c.type
  nam = c.name
  try:
    typnam = str(typ)
  except Exception as err:
    plpy.warning(err)
    typnam = 'text'
  plpy.debug('c.type is of type ' + typnam)
  typnam = typnam.split('(')[0] # strip any type modifier
  query += sep + nam + ' ' + typnam
  sep = ', '
query += ' ) SERVER ' + plpy.quote_ident(fdwsrvnam)

plpy.debug('TABLE QUERY: ' + query)
plpy.execute(query)

return 'public.' + fdwtblnam

$$ LANGUAGE 'plpythonu' VOLATILE;

REVOKE EXECUTE ON FUNCTION CDB_CreateForeignTable(text,text) FROM public;

-- Drop a foreign table and the relative foreign server (and mappings)
--
-- @param tablename name or oid of the foreign table
--
CREATE OR REPLACE FUNCTION CDB_DropForeignTable(thetab regclass)
RETURNS void AS $$
DECLARE
  rec RECORD;
BEGIN
  SELECT INTO rec s.srvname, count(t2.ftrelid) FROM 
      pg_catalog.pg_foreign_table t, pg_catalog.pg_foreign_server s,
      pg_catalog.pg_foreign_table t2 WHERE t.ftrelid = thetab
      AND s.oid = t.ftserver AND t2.ftserver = s.oid GROUP BY s.srvname;
  IF rec.srvname IS NULL THEN
    RAISE EXCEPTION 'Table "%" is not a foreign table', thetab;
  END IF;

  RAISE DEBUG 'Server name: %, tables; %', rec.srvname, rec.count;

  EXECUTE 'DROP FOREIGN TABLE ' || thetab::text;

  IF rec.count > 1 THEN
    RAISE EXCEPTION 'Server "%" of table "%" is used for other tables',
      rec.srvname, thetab;
  END IF;

  EXECUTE 'DROP SERVER ' || rec.srvname || ' CASCADE';

END;
$$ LANGUAGE 'plpgsql' VOLATILE;
