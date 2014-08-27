-- width is cell width, height is cell height
CREATE OR REPLACE FUNCTION CDB_ClipToGrid(geom GEOMETRY, width FLOAT8, height FLOAT8, origin GEOMETRY DEFAULT NULL)
RETURNS SETOF GEOMETRY
AS $$
DECLARE
  h GEOMETRY; -- rectangle cell
  clip GEOMETRY; -- clipped geometry
  hstep FLOAT8; -- horizontal step
  vstep FLOAT8; -- vertical step
  hw FLOAT8; -- half width
  hh FLOAT8; -- half height
  vstart FLOAT8;
  hstart FLOAT8;
  hend FLOAT8;
  vend FLOAT8;
  xoff FLOAT8;
  yoff FLOAT8;
  xgrd FLOAT8;
  ygrd FLOAT8;
  x FLOAT8;
  y FLOAT8;
  srid INTEGER;
BEGIN

  srid := ST_SRID(geom);

  xoff := 0; 
  yoff := 0;

  IF origin IS NOT NULL THEN
    IF ST_SRID(origin) != srid THEN
      RAISE EXCEPTION 'SRID mismatch between geometry (%) and origin (%)', srid, ST_SRID(origin);
    END IF;
    xoff := ST_X(origin);
    yoff := ST_Y(origin);
  END IF;

  --RAISE DEBUG 'X offset: %', xoff;
  --RAISE DEBUG 'Y offset: %', yoff;


  hw := width/2.0;
  hh := height/2.0;

  xgrd := hw;
  ygrd := hh;
  --RAISE DEBUG 'X grid size: %', xgrd;
  --RAISE DEBUG 'Y grid size: %', ygrd;

  hstep := width;
  vstep := height;

  --RAISE DEBUG 'xoff: %', xoff;
  --RAISE DEBUG 'st_xmin(geom): %', st_xmin(geom);

  -- Tweak horizontal start on hstep grid from origin 
  hstart := xoff + ceil((ST_XMin(geom)-xoff)/hstep)*hstep - hstep;
  --RAISE DEBUG 'hstart: %', hstart;

  -- Tweak vertical start on vstep grid from origin 
  vstart := yoff + ceil((ST_Ymin(geom)-yoff)/vstep)*vstep - vstep;
  --RAISE DEBUG 'vstart: %', vstart;

  hend := ST_XMax(geom);
  vend := ST_YMax(geom);

  --RAISE DEBUG 'hend: %', hend;
  --RAISE DEBUG 'vend: %', vend;

  x := hstart;
  WHILE x < hend LOOP -- over X
    y := vstart;
    h := ST_MakeEnvelope(x, y, x+width, y+height, srid);
    WHILE y < vend LOOP -- over Y
      -- TODO: early out if no intersection ?
      clip := ST_Intersection(h, geom);
      IF NOT ST_IsEmpty(clip) THEN
        RETURN NEXT clip;
      ELSE
        --RAISE DEBUG ' intersection is empty';
        --RAISE DEBUG '%', st_astext(h);
        --RAISE DEBUG '%', st_astext(geom);
      END IF;
      h := ST_Translate(h, 0, vstep);
      y := yoff + round(((y + vstep)-yoff)/ygrd)*ygrd; -- round to grid
    END LOOP;
    x := xoff + round(((x + hstep)-xoff)/xgrd)*xgrd; -- round to grid
  END LOOP;

  RETURN;
END
$$ LANGUAGE 'plpgsql' IMMUTABLE;
