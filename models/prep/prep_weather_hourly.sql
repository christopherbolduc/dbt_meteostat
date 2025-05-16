WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        , TO_CHAR(time,'HH24:MI') as hour           -- time (hours:minutes) as TEXT dtype
        , TO_CHAR(date, 'FMMonth') AS month_name    -- month name as a TEXT dtype
        , TO_CHAR(date,'FMDay') AS weekday          -- weekday name as TEXT dtype      
        , DATE_PART('day', date) AS date_day
		, DATE_PART('month', date) AS date_month
		, DATE_PART('year',date) AS date_year
		, DATE_PART('week', date) AS cw
    FROM hourly_data hd
),
add_more_features AS (
    SELECT *
		,CASE 
			WHEN EXTRACT(HOUR FROM time) BETWEEN 0 AND 5 THEN 'night'
			WHEN EXTRACT(HOUR FROM time) BETWEEN 6 AND 17 THEN 'day'
			WHEN EXTRACT(HOUR FROM time) BETWEEN 18 AND 23 THEN 'evening'
		END AS day_part
    FROM add_features 
)
SELECT *
FROM add_more_features