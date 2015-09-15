--Drop exisiting od related objects
DROP TABLE IF EXISTS public.est_pop CASCADE;

--Create od table and index
CREATE TABLE public.est_pop
(
  zone_id integer NOT NULL,
  zone_id double precision,
  ald0_6  double precision,
  ald7_15 double precision,
  ald16_19 double precision,
  ald20_24 double precision,
  ald25_44 double precision,
  ald45_64 double precision,
  ald65_w double precision,
  totbef double precision,
  CONSTRAINT est_pop_pkey PRIMARY KEY (zone_id)
);

--debug view for viewing OD flows in GIS tools
CREATE OR REPLACE VIEW est_pop_geom AS 
 SELECT *
   FROM est_pop
   JOIN pop_statistic
    ON est_pop.zone_id = pop_statistic.id;