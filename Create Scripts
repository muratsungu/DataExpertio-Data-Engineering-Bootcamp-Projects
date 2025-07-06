CREATE TYPE films AS
(
	year integer,
	filmid text,
	film text,
	votes integer,
	rating real
);

CREATE TYPE quality_class AS ENUM
    ('star', 'good', 'average', 'bad');

CREATE TABLE actors
(
    actorid text,
    actor text,
    active_year integer,
    films films[],
    quality_class quality_class,
    is_active boolean,
    CONSTRAINT actors_pkey PRIMARY KEY (actorid, active_year)
)

CREATE TABLE actors_history_scd
(
    actorid text,
    actor text,
    quality_class quality_class,
    is_active boolean,
    start_date integer,
    end_date integer,
    CONSTRAINT actors_history_scd_pkey PRIMARY KEY (actorid, start_date)
)
