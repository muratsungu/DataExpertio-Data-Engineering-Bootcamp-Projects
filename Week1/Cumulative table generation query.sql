
--
CREATE OR REPLACE PROCEDURE actors_yearly_pipeline(
	IN active_yearx integer,  -- The current year to process (e.g., 2020)
	IN last_yearx integer)    -- The previous year to reference (e.g., 2019)
LANGUAGE 'sql'
AS $BODY$
-- Step 1: Get last year's actor data from the 'actors' table
with cte as (
select af.actorid,af.actor,af.active_year,af.films
from actors af
where active_year = last_yearx
),
-- Step 2: Aggregate current year's films per actor from 'actor_films'
cte2 as (
select af.actorid,af.actor,af.year,array_agg(row(af.year,af.filmid,af.film,af.votes,af.rating)::films) films
from actor_films af
where year = active_yearx
group by af.actorid,af.actor,af.year
),
-- Step 3: Merge previous and current year data
    -- If an actor existed last year but not this year, they are inactive
    -- If they appear this year, they are active and their film history is updated
cte3 as (
select   coalesce(t.actorid,y.actorid) as actorid
		,coalesce(t.actor,y.actor) as actor
		,active_yearx as year
		,case when y.films is null
		      then t.films
			  else y.films || t.films end
		 as films
		,case when t.actorid is not null then True Else False End is_active
from cte2 as t
full outer join cte as y on t.actorid = y.actorid
),
-- Step 4: Calculate average rating per actor based on their film list
cte4 as (
SELECT
    actorid,
    (
        SELECT AVG(f.rating)
        FROM unnest(films) AS f
    ) AS avg_rating
FROM cte3
)
-- Step 5: Insert the processed data into the 'actors' table
-- Assign a quality_class based on average rating
insert into actors
select x.actorid
	  ,x.actor
	  ,x.year
	  ,x.films
	  ,CASE WHEN y.avg_rating > 8 THEN 'star'
				WHEN y.avg_rating > 7 THEN 'good'
				WHEN y.avg_rating > 6 THEN 'average'
				ELSE 'bad' END::quality_class
	  ,x.is_active			
from cte3 x
join cte4 y on x.actorid = y.actorid
$BODY$;
