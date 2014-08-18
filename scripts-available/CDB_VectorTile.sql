--
-- NOTE:
--
--  1. Taking a 1-time configuration state for an aggregate is not good
--     because it invokes the function creating the configuration once
--     for every record.
--
--  2. Using the GD python dictionary is not good because all invocations
--     of the aggregate within the same session share the same variable
--     (each being in a transaction should make it safer but still direct
--     calls to the sate function could pollute next aggregate invocation)
--
--
--

DROP TYPE IF EXISTS CDB_VectorTile_Feature CASCADE;

DROP TYPE IF EXISTS CDB_VectorTile_Layer CASCADE;
CREATE TYPE CDB_VectorTile_Layer AS (
  -- Layer configuration
  ver int,
  name text,
  ipx float8,
  ipy float8,
  sfx float8,
  sfy float8,
  tol float8,
  flags int,    -- 1:inc_geom_types 2:inc_geom_id 
  ext geometry  -- cropping tile extent, if not null
);

CREATE OR REPLACE FUNCTION CDB_MakeEmptyVectorTile_Layer(
  ver int, name text, ipx float8, ipy float8,
  sfx float8, sfy float8, tol float8,
  flags int DEFAULT 0, -- 1:inc_geom_types 2:inc_geom_id
  ext geometry DEFAULT NULL -- nominal tile extent
) RETURNS CDB_VectorTile_Layer AS $$
  --DO $x$ plpy.notice('CDB_MakeEmptyVectorTile_Layer called') $x$ language 'plpythonu';
  SELECT (
    ver, name, ipx, ipy, sfx, sfy, tol, flags, ext
    --null, null, null
  )::CDB_VectorTile_Layer;
$$ LANGUAGE 'sql';

DROP FUNCTION IF EXISTS _CDB_VectorTile_Layer_addFeature(
  in_layer CDB_VectorTile_Layer, -- state
  layer_check CDB_VectorTile_Layer, -- the initially provided one ?
  geom geometry,    -- feature geometry
  id bigint,        -- feature identifier 
  text[][] -- feature attributes
);

CREATE OR REPLACE FUNCTION _CDB_VectorTile_Layer_addFeature(
  in_layer bytea, -- state (fake)
  layer_check CDB_VectorTile_Layer, -- the initially provided one ?
  geom geometry,    -- feature geometry
  id bigint,        -- feature identifier 
  attribute_names  text[], -- feature attribute names
  attribute_values text[], -- feature attribute values
  attribute_types  text[]  -- feature attribute types
) RETURNS bytea
AS $$

  # 0. check arguments
  if 'layer' in GD:
    layer = GD['layer']
  else:
    layer = GD['layer'] = {} # layer_check
    layer['features'] = []
    layer['keys'] = []
    layer['vals'] = []
    for k in ['ver','name','ipx','ipy','sfx','sfy','tol','ext','flags']:
      layer[k] = layer_check[k]

  if attribute_names and attribute_values:
    if len(attribute_names) != len(attribute_values):
      plpy.error('Number of attribute names and values should match')

#  plpy.notice('Layer ' + layer['name'] + ' with version ' \
#    + str(layer['ver']) + ' has ' \
#    + str(len(layer['features'])) + ' features')

  feature = {}

  # 1. append encoded geometry to feature
  plan = plpy.prepare(\
    "SELECT ST_AsVectorTile_Geometry($1, $2, $3, $4, $5, $6, $7) g", \
    [ "geometry","float8","float8","float8","float8","float8","geometry" ])
  res = plpy.execute(plan, [ geom, layer['ipx'], layer['ipy'], \
                            layer['sfx'], layer['sfy'], \
                            layer['tol'], layer['ext'] ])
  encgeom = res[0]['g']
  feature['geom'] = encgeom

  # 2. append id to layer.feature_id array
  feature['id'] = id

  # 3. append geometry type to layer.feature_type array
  plan = plpy.prepare('SELECT GeometryType($1) t', ['geometry'])
  res = plpy.execute(plan, [geom])
  typname = res[0]['t']
  #plpy.notice("type is " + typname)
  if typname == 'POINT':
    typid = 1
  elif typname == 'LINESTRING':
    typid = 2
  elif typname == 'POLYGON':
    typid = 3
  else:
    typid = 0 # UNKNOWN/unsupported
  feature['gtyp'] = typid

  # Ensure an element is in the cont list, and return its index (0-based)
  def getindex(cont, elem):
    try:
      idx = cont.index(elem)
    except ValueError:
      cont.append(elem)
      idx = len(cont)-1
    return idx

  # 4. resolve attributes as an array of key/value identifiers, populating
  #    layer.keys and layer.vals and incrementing layer.nkeys and layer.nvals
  #    as needed
  #plpy.notice("attributes are: " + str(attribute_values) + " of type " + str(type(attribute_values)))
  tags = []
  if attribute_values:
    for i in range(0, len(attribute_values)):
      fnam = attribute_names[i]
      fval = attribute_values[i]
      fnam_tag = getindex(layer['keys'], fnam)
      fval_tag = getindex(layer['vals'], fval)
      #plpy.notice(" attname " + fnam + " tag is " + str(fnam_tag))
      #plpy.notice(" attval " + fval + " tag is " + str(fval_tag))
      tags.append([fnam_tag,fval_tag])

  # 5. append the array of key/value identifiers to layer.feature_tags
  feature['tags'] = tags

  # 6. append the new feature to the layer
  layer['features'].append(feature)

  # Fake return
  return 1

$$ LANGUAGE 'plpythonu' IMMUTABLE;

DROP FUNCTION IF EXISTS _CDB_VectorTile_Layer_encode(layer bytea);
CREATE OR REPLACE FUNCTION _CDB_VectorTile_Layer_encode(layer bytea)
RETURNS bytea -- {
AS $$
  if not 'layer' in GD: return 
  layer = GD['layer']
  del GD['layer']

  # See https://developers.google.com/protocol-buffers/docs/encoding

  plpy.notice('ENCODE: layer ' + layer['name'] + ' has ' \
    + str(len(layer['features'])) + ' features, ' \
    + str(len(layer['keys'])) + ' keys, ' \
    + str(len(layer['vals'])) + ' vals')

  # q is the value, b is the output bytearray
  def encode_varint_uint32(b, q):
    n = 0
    while ((q>>(7*(n+1))) > 0):
      grp=128^(127&(q>>(7*n)))
      #plpy.notice("*grp is " + str(grp));
      b += chr( grp )
      n = n + 1
      #plpy.notice('___ b is: ' + b.encode('hex'))
    grp = 127 & (q>>(7*n))
    #plpy.notice("grp is " + str(grp));
    b += chr( grp )
    #plpy.notice("grp was done " + str(grp));
    #plpy.notice('out of encode_varint_uint32 b is: ' + b.encode('hex'))
    return b

  def encode_msg_uint32(b, tag, val):
    b += chr( tag << 3 ) # | 0, for a varint
    #plpy.notice('after encode_msg_uint32 tagging b is: ' + b.encode('hex'))
    val = ( (val << 1) ^ (val >> 31) ) # zig-zag encoding
    #plpy.notice('zigzag val is ' + str(val))
    b = encode_varint_uint32(b, val)
    #plpy.notice('out of encode_msg_uint32 b is: ' + b.encode('hex'))
    return b

  def encode_msg_enum(b, tag, val):
    b += chr( tag << 3 ) # | 0, for a varint
    #plpy.notice('after encode_msg_uint32 tagging b is: ' + b.encode('hex'))
    b = encode_varint_uint32(b, val)
    #plpy.notice('out of encode_msg_enum b is: ' + b.encode('hex'))
    return b

  def encode_msg_string(b, tag, val):
    b += chr( (tag << 3) | 2 ) # 2 is length-delimited
    #plpy.notice('before encoding len: ' + b.encode('hex'))
    b = encode_varint_uint32(b, len(val))
    #plpy.notice(' after encoding len: ' + b.encode('hex'))
    # append each byte from the string
    u = bytes(val)
    b += u
    #plpy.notice('out of encode_msg_string b (len ' + str(len(val)) + ') is: ' + b.encode('hex'))
    return b

  def encode_msg_feature(b, tag, f, flags):

    fb = bytes()

    # optional uint64 id = 1 [ default = 0 ];
    if ( flags & 2 ): # include geom id
      #plpy.notice("encoding id " + str(f['id']))
      fb = encode_msg_uint32(fb, 1, f['id'])

    # repeated uint32 tags = 2 [ packed = true ];
    #plpy.notice("encoding tags")
    tags = f['tags']
    if len(tags):
      tb = bytes()
      for t in tags:
        tb = encode_varint_uint32(tb, t[0]) # key
        tb = encode_varint_uint32(tb, t[1]) # val
      fb += chr( (2 << 3) | 2 ) # 2 is for length-delimited, and 2 is tag
      fb = encode_varint_uint32(fb, len(tb))
      fb += tb

    # optional GeomType type = 3 [ default = UNKNOWN ];
    if ( flags & 1 ): # include geom type
      fb = encode_msg_enum(fb, 3, f['gtyp'])

    # repeated uint32 geometry = 4 [ packed = true ];
    l = len(f['geom'])
    if l:
      #plpy.notice('Geom encoded size is : ' + str(l))
      fb += chr( (4 << 3) | 2 ) # 2 is for length-delimited, and 4 is tag
      fb = encode_varint_uint32(fb, l)
      fb += f['geom']

    b += chr( (tag << 3) | 2 ) # 2 is for length-delimited
    b = encode_varint_uint32(b, len(fb))
    b += fb

    return b

  b = bytes()

  # required uint32 version = 15 [ default = 1 ];
  b = encode_msg_uint32(b, 15, layer['ver'])
  #plpy.notice('After version: ' + b.encode('hex'))

  # required string name = 1;
  b = encode_msg_string(b, 1, layer['name'])
  #plpy.notice('After name: ' + b.encode('hex'))

  # repeated Feature features = 2;
  for k in layer['features']:
    b = encode_msg_feature(b, 2, k, layer['flags'])
  #plpy.notice('After features, buffer is: ' + b.encode('hex'))

  # repeated string keys = 3;
  for k in layer['keys']:
    b = encode_msg_string(b, 3, k)
  #plpy.notice('After keys, buffer is: ' + b.encode('hex'))

  # repeated Value values = 4;
  for k in layer['vals']:
    b = encode_msg_string(b, 4, k)
  #plpy.notice('After vals, buffer is: ' + b.encode('hex'))

  # optional uint32 extent = 5 [ default = 4096 ];

  return b;
$$ LANGUAGE 'plpythonu' IMMUTABLE; -- }

DROP AGGREGATE IF EXISTS
 CDB_AsVectorTile_Layer(CDB_VectorTile_Layer,
                        geometry, bigint, text[], text[], text[]);

-- Return an encoded VectorTile.Layer message
-- In order to turn into a full VectorTile the caller
-- needs to prefix the value returned by the aggregate
-- with tag=3 and size of following layers set 
CREATE AGGREGATE CDB_AsVectorTile_Layer(
  -- This part must be immutable
  CDB_VectorTile_Layer,
  -- This part is meant to be variable
  geometry, -- feature geometry
  bigint,   -- feature identifier
  text[],   -- feature attribute names
  text[],   -- feature attribute values
  text[]    -- feature attribute types
) (
  SFUNC = _CDB_VectorTile_Layer_addFeature,
  STYPE = bytea, --CDB_VectorTile_Layer,
  FINALFUNC = _CDB_VectorTile_Layer_encode
  -- [ , INITCOND = initial_condition ]
  -- [ , SORTOP = sort_operator ]
);


-- Test !
\set VERBOSITY terse
SELECT octet_length(CDB_AsVectorTile_Layer(
  CDB_MakeEmptyVectorTile_Layer(
    1,   -- version
    'l', -- name
    0,   -- ipx
    0,   -- ipy
    1,   -- sfx
    1,   -- sfy
    0,   -- tolerance
    1|2  -- flags
  ),
  g, -- geometry
  i, -- feature_id
  ARRAY['a1','a2'],    -- attribute_names
  ARRAY[a1,a2],        -- attibute_values
  ARRAY['text','text'] -- attribute_types
)) FROM (
 VALUES ( 'POINT(0 0)'  ::geometry, 1, 'a', 'A' )
       ,( 'POINT(1 2)'  ::geometry, 2, 'b', 'B' )
       ,( 'POINT(-1 -1)'::geometry, 3, 'c', 'C' )
       ,( '0101000000000008070E9013400000B8363EA51340', 75757777, 'd','D')
       ,( 'POINT(0 0)', 75757777, 'd','D')
) as foo(g,i,a1,a2);
