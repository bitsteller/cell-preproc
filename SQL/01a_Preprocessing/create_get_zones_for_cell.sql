-- get_cells_for_cell(cellid) searches for all statistic zones that intersect with the voronoi cell
-- returns a table containing all intersecting zone and their share of the area of the cell that they cover
-- the sum of all shares adds up to 1.0

DROP FUNCTION get_zones_for_cell(bigint) CASCADE;

CREATE OR REPLACE FUNCTION get_zones_for_cell(bigint) RETURNS TABLE(zone_id integer, area_share double precision, pop_share double precision) AS $$
BEGIN
  RETURN QUERY 
  WITH cell AS (SELECT *, ST_AREA(voronoi.geom) AS area FROM voronoi WHERE id = $1),
     zones AS (SELECT pop_statistic.id AS id, pop_statistic.geom AS geom, pop_statistic.totbef FROM cell, pop_statistic WHERE ST_Intersects(cell.geom, pop_statistic.geom))
  SELECT  zone.id AS zone_id, 
          ST_AREA(ST_INTERSECTION(zone.geom, cell.geom))/cell.area AS area_share, 
          (CASE WHEN (SELECT SUM(zones.totbef) FROM zones) > 0 THEN
            zone.totbef/(SELECT SUM(zones.totbef) FROM zones)
          ELSE
            1/(SELECT COUNT(*) FROM zones) --split up equally if no population data available
          END) AS pop_share
  FROM cell, zones AS zone;
  RETURN;
END
$$ LANGUAGE plpgsql STABLE;

--cache cell mapping in materialized view for performance
DROP MATERIALIZED VIEW IF EXISTS cell_zones CASCADE;

CREATE MATERIALIZED VIEW cell_zones AS 
 SELECT voronoi.id AS cell_id,
    get_zones_for_cell.zone_id,
    get_zones_for_cell.area_share,
    get_zones_for_cell.pop_share
   FROM voronoi,
    LATERAL get_zones_for_cell(voronoi.id) get_zones_for_cell(zone_id, area_share, pop_share)
WITH DATA;

CREATE INDEX cell_zones_cell_id_idx
  ON public.cell_zones
  USING btree
  (cell_id);