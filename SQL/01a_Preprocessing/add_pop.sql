-- UPSERTs popuplation statistics
-- adds a population (different age groups and total) to the zone

WITH new_values (zone_id, population) as (
  values 
     (%(zone_id)s, %(population)s)
),
upsert as
( 
    update est_pop m 
        set population = m.population + nv.population
    FROM new_values nv
    WHERE m.zone_id = nv.zone_id
    RETURNING m.*
)
INSERT INTO est_pop (zone_id, population)
SELECT *
FROM new_values
WHERE NOT EXISTS (SELECT 1 
                  FROM est_pop up 
                  WHERE up.zone_id = new_values.zone_id)