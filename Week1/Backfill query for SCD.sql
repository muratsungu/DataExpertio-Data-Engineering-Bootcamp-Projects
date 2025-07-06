-- Step 1: Detect changes in actor attributes (is_active or quality_class) over time
WITH cte AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        active_year,
        
        -- Flag a change if either 'is_active' or 'quality_class' differs from the previous year
        CASE 
            WHEN is_active <> LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY active_year)
              OR quality_class <> LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY active_year)
            THEN 1 
            ELSE 0 
        END AS change_ind
    FROM actors
    WHERE active_year < 2021  -- Only consider data before 2021 for historical processing
),

-- Step 2: Assign a group number to each continuous segment of unchanged attributes
cte2 AS (
    SELECT *,
        -- Cumulative sum of change indicators to group consecutive identical records
        SUM(change_ind) OVER (PARTITION BY actorid ORDER BY active_year) AS change_group
    FROM cte
)

-- Step 3: Insert grouped historical records into the SCD table
INSERT INTO actors_history_scd
SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    MIN(active_year) AS start_date,         -- Start of the unchanged period
    MAX(active_year) + 1 AS end_date        -- End is exclusive, so add 1
FROM cte2
GROUP BY 
    actorid,
    actor,
    is_active,
    quality_class,
    change_group                             -- Group by change group to isolate stable periods
ORDER BY 
    actorid,
    change_group;
