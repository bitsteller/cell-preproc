CREATE INDEX pop_statistic_geom_idx ON pop_statistic USING gist(geom);

CREATE TABLE public.pop_statistic_fixed
(
  id integer NOT NULL,
  geom geometry(geometry,4326),
  rutstorl double precision,
  ruta character varying(13),
  ald0_6 double precision,
  ald7_15 double precision,
  ald16_19 double precision,
  ald20_24 double precision,
  ald25_44 double precision,
  ald45_64 double precision,
  ald65_w double precision,
  totbef double precision
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.pop_statistic_fixed
  OWNER TO d4d;


INSERT INTO pop_statistic_fixed
WITH containing AS
(
SELECT a.id AS aid, b.id AS bid 
FROM pop_statistic a, pop_statistic b
WHERE a.rutstorl > b.rutstorl AND a.id <> b.id AND a.geom && a.geom 
AND ST_Contains(ST_Buffer(a.geom,0.0001), b.geom)
)

SELECT a.id, ST_Multi(ST_Difference(a.geom, ST_Union(b.geom))) AS geom, a.rutstorl, a.ruta, a.ald0_6, a.ald7_15, a.ald16_19, a.ald20_24, a.ald25_44, a.ald45_64, a.ald65_w, a.totbef
FROM containing, pop_statistic a, pop_statistic b
WHERE a.id = aid AND b.id = bid
GROUP BY aid, a.id, a.geom, a.rutstorl, a.ruta, a.ald0_6, a.ald7_15, a.ald16_19, a.ald20_24, a.ald25_44, a.ald45_64, a.ald65_w, a.totbef;


INSERT INTO pop_statistic_fixed
WITH containing AS
(
SELECT a.id AS aid, b.id AS bid 
FROM pop_statistic a, pop_statistic b
WHERE a.rutstorl > b.rutstorl AND a.id <> b.id AND a.geom && a.geom 
AND ST_Contains(ST_Buffer(a.geom,0.0001), b.geom)
)
SELECT *
FROM pop_statistic a
WHERE NOT EXISTS(SELECT * FROM containing WHERE a.id = containing.aid);