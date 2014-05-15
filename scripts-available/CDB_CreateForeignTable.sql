-- Create a foreign server, user mapping and table
--
-- @param db_url an sqlalchemy connection string, like:
--               mysql://<user>:<password>@<host>/<dbname>
-- @param tablename name of the remote table
--
-- @return name of local table
--
CREATE OR REPLACE FUNCTION CDB_CreateForeignTable(db_url text, tablename text)
RETURNS text AS $$

#--
#--  TODO: Rate-limit !
#--

#--
#--  Find remote table structure
#--

from sqlalchemy import create_engine
from sqlalchemy.exc import InterfaceError, NoSuchTableError, OperationalError
from sqlalchemy.schema import Table, Column, MetaData

#---- @{ Spatial integration ----

from sqlalchemy.types import UserDefinedType
class Geometry(UserDefinedType):
  name = 'GEOMETRY'
  def get_col_spec(self):
    return '%s' % (self.name)

# PostgreSQL
from sqlalchemy.dialects.postgresql.base import ischema_names
ischema_names['geometry'] = Geometry

# MySQL
from sqlalchemy.dialects.mysql.base import ischema_names
ischema_names['geometry'] = Geometry

#---- Spatial integration @}----

try:
  metadata = MetaData(db_url)
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

plpy.info('Table ' + str(tableobj) + ' cols: ')
for c in tableobj.columns:
 typ = c.type
 nam = c.name
 plpy.debug('c.type is of type ' + str(type(c.type)))
 try:
  typnam = str(typ)
 except Exception as err:
  plpy.warning(err)
  typnam = 'text'
 plpy.info(' ' + nam + ' ' + typnam)

#--
#-- TODO: Create server (or should we reuse existing ones?)
#--

#--
#-- TODO: Create user mapping (to hide auth)
#--

#--
#-- TODO: Create foreign table (pay attention to spatial columns)
#--

return 'tadah!'

$$ LANGUAGE 'plpythonu' VOLATILE;

REVOKE EXECUTE ON FUNCTION CDB_CreateForeignTable(text,text) FROM public;

