--Drop exisiting od related objects
DROP TABLE IF EXISTS public.est_pop CASCADE;

--Create od table and index
CREATE TABLE public.est_pop
(
  zone_id integer NOT NULL,
  population double precision,
  CONSTRAINT est_pop_pkey PRIMARY KEY (zone_id)
);

--debug view for viewing OD flows in GIS tools
CREATE OR REPLACE VIEW est_pop_geom AS 
 SELECT *
   FROM est_pop
   JOIN pop_statistic
    ON est_pop.zone_id = pop_statistic.id;