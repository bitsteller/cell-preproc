DROP MATERIALIZED VIEW IF EXISTS cell_est_pop;

CREATE MATERIALIZED VIEW cell_est_pop AS (
SELECT antenna_id, SUM(hits) FROM ehomebase
GROUP BY antenna_id
);