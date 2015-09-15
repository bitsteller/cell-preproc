-- UPSERTs popuplation statistics
-- adds a population (different age groups and total) to the zone
-- if a row for the given od pair does not yet exist, a new row is created (UPSERT)

WITH new_values (zone_id, ald0_6, ald7_15, ald16_19, ald20_24, ald25_44, ald45_64, ald65_w, totbef) as (
  values 
     (%(antenna_id)s, %(ald0_6)s, %(ald7_15)s, %(ald16_19)s, %(ald20_24)s, %(ald25_44)s, %(ald45_64)s, %(ald65_w)s, %(totbef)s)
),
upsert as
( 
    update od m 
        set flow = m.flow + nv.flow
    FROM new_values nv
    WHERE m.zone_id
    RETURNING m.*
)
INSERT INTO est_pop (zone_id, ald0_6, ald7_15, ald16_19, ald20_24, ald25_44, ald45_64, ald65_w, totbef)
SELECT *
FROM new_values
WHERE NOT EXISTS (SELECT 1 
                  FROM od up 
                  WHERE up.zone_id = new_values.zone_id)