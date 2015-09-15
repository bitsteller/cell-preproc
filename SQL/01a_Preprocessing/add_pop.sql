-- UPSERTs popuplation statistics
-- adds a population (different age groups and total) to the zone
-- if a row for the given od pair does not yet exist, a new row is created (UPSERT)

WITH new_values (zone_id, population) as (
  values 
     (%(zone_id)s, %(population)s)
),
upsert as
( 
    update od m 
        set flow = m.population + nv.population
    FROM new_values nv
    WHERE m.zone_id
    RETURNING m.*
)
INSERT INTO est_pop (zone_id, population)
SELECT *
FROM new_values
WHERE NOT EXISTS (SELECT 1 
                  FROM od up 
                  WHERE up.zone_id = new_values.zone_id)