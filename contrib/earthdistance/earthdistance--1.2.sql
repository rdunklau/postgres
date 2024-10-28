/* contrib/earthdistance/earthdistance--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION earthdistance" to load this file. \quit

-- earth() returns the radius of the earth in meters. This is the only
-- place you need to change things for the cube base distance functions
-- in order to use different units (or a better value for the Earth's radius).

CREATE FUNCTION earth() RETURNS float8
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
RETURN '6378168'::float8;

-- Astronomers may want to change the earth function so that distances will be
-- returned in degrees. To do this comment out the above definition and
-- uncomment the one below. Note that doing this will break the regression
-- tests.
--
-- CREATE FUNCTION earth() RETURNS float8
-- LANGUAGE SQL IMMUTABLE
-- AS 'SELECT 180/pi()';

-- Define domain for locations on the surface of the earth using a cube
-- datatype with constraints. cube provides 3D indexing.
-- The cube is restricted to be a point, no more than 3 dimensions
-- (for less than 3 dimensions 0 is assumed for the missing coordinates)
-- and that the point must be very near the surface of the sphere
-- centered about the origin with the radius of the earth.

CREATE DOMAIN earth AS $extschema:cube@.cube
  CONSTRAINT not_point check($extschema:cube@.cube_is_point(value))
  CONSTRAINT not_3d check($extschema:cube@.cube_dim(value) <= 3)
  CONSTRAINT on_surface check(abs($extschema:cube@.cube_distance(value, '(0)'::$extschema:cube@.cube) /
  earth() - '1'::float8) < '10e-7'::float8);

CREATE FUNCTION sec_to_gc(float8)
RETURNS float8
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN CASE WHEN $1 < 0 THEN 0::float8 WHEN $1/(2*earth()) > 1 THEN pg_catalog.pi()*earth() ELSE 2*earth()*pg_catalog.asin($1/(2*earth())) END;

CREATE FUNCTION gc_to_sec(float8)
RETURNS float8
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN CASE WHEN $1 < 0 THEN 0::float8 WHEN $1/earth() > pg_catalog.pi() THEN 2*earth() ELSE 2*earth()*pg_catalog.sin($1/(2*earth())) END;

CREATE FUNCTION ll_to_earth(float8, float8)
RETURNS earth
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN @extschema:cube@.cube(@extschema:cube@.cube(@extschema:cube@.cube(earth()*pg_catalog.cos(radians($1))*pg_catalog.cos(radians($2))),earth()*pg_catalog.cos(radians($1))*pg_catalog.sin(radians($2))),earth()*pg_catalog.sin(radians($1)))::earth;

CREATE FUNCTION latitude(earth)
RETURNS float8
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN CASE WHEN @extschema:cube@.cube_ll_coord($1, 3)/earth() < -1 THEN -90::float8 WHEN @extschema:cube@.cube_ll_coord($1, 3)/earth() > 1 THEN 90::float8 ELSE pg_catalog.degrees(pg_catalog.asin(@extschema:cube@.cube_ll_coord($1, 3)/earth())) END;

CREATE FUNCTION longitude(earth)
RETURNS float8
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN pg_catalog.degrees(pg_catalog.atan2(@extschema:cube@.cube_ll_coord($1, 2), @extschema:cube@.cube_ll_coord($1, 1)));

CREATE FUNCTION earth_distance(earth, earth)
RETURNS float8
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN sec_to_gc(@extschema:cube@.cube_distance($1, $2));

CREATE FUNCTION earth_box(earth, float8)
RETURNS cube
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
RETURN @extschema:cube@.cube_enlarge($1, gc_to_sec($2), 3);

--------------- geo_distance

CREATE FUNCTION geo_distance (point, point)
RETURNS float8
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE AS 'MODULE_PATHNAME';

--------------- geo_distance as operator <@>

CREATE OPERATOR <@> (
  LEFTARG = point,
  RIGHTARG = point,
  PROCEDURE = geo_distance,
  COMMUTATOR = <@>
);
