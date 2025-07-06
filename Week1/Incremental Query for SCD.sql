-- Step 1: Identify brand new actors who have no history in the SCD table
WITH insert_new AS (
    SELECT actorid, actor, quality_class, is_active, active_year AS start_date, active_year + 1 AS end_date
    FROM actors x
    WHERE x.active_year = 2021
      -- Only include actors who do not exist in the history table at all
      AND NOT EXISTS (
          SELECT 1 FROM actors_history_scd y WHERE x.actorid = y.actorid
      )
),
-- Step 2: Identify actors who already exist in the history table,
-- but whose attributes (quality_class or is_active) have changed in 2021
insert_new_metrics AS (
    SELECT actorid, actor, quality_class, is_active, active_year AS start_date, active_year + 1 AS end_date
    FROM actors x
    WHERE x.active_year = 2021
      AND EXISTS (
          SELECT 1
          FROM actors_history_scd y
          WHERE x.actorid = y.actorid
            AND y.end_date = 2021
            AND (x.quality_class <> y.quality_class OR x.is_active <> y.is_active)
      )
),
-- Step 3: Identify actors whose attributes have not changed,
-- but whose previous record needs to be updated (end_date updated)
update_end_date AS (
    SELECT x.actorid, x.actor, y.start_date, x.active_year + 1 AS end_date
    FROM actors x
    JOIN actors_history_scd y
      ON x.actorid = y.actorid
     AND y.end_date = 2021
     AND x.quality_class = y.quality_class
     AND x.is_active = y.is_active
    WHERE x.active_year = 2021
),
-- Step 4: Insert new records into the SCD table
-- Includes both brand new actors and those with changed attributes
do_insert AS (
    INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date)
    SELECT *
    FROM (
        -- Combine both new and changed actors
        SELECT * FROM insert_new
        UNION
        SELECT * FROM insert_new_metrics
    ) AS combined
    -- Avoid inserting duplicates if the same actor/start_date already exists
    WHERE NOT EXISTS (
        SELECT 1
        FROM actors_history_scd ah
        WHERE ah.actorid = combined.actorid
          AND ah.start_date = combined.start_date
    )
    RETURNING 1 -- Dummy return to keep the CTE chain valid
)
-- Step 5: Update the end_date of existing records for unchanged actors
-- This "closes" the previous record by extending its end_date
UPDATE actors_history_scd
SET end_date = b.end_date
FROM update_end_date b
WHERE actors_history_scd.actorid = b.actorid
  AND actors_history_scd.start_date = b.start_date;
