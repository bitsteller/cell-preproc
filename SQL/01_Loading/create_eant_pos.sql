--Delete voronoi related objects
DROP TABLE IF EXISTS public.eant_pos CASCADE;

DROP SEQUENCE IF EXISTS public.eant_pos_antenna_id_seq;

CREATE SEQUENCE public.eant_pos_antenna_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1480
  CACHE 1;

--Create table voronoi
CREATE TABLE public.eant_pos
(
  name character varying,
  place character varying,
  trash character varying,
  lon real,
  lat real,
  place2 character varying,
  code character varying,
  code_city character varying,
  region character varying,
  mun character varying,
  country integer,
  num integer,
  geom geometry(Point,4326),
  id integer NOT NULL DEFAULT nextval('eant_pos_antenna_id_seq'::regclass)
);