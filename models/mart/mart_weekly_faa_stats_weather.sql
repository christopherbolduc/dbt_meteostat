WITH daily_weather AS (                 -- Daily weather data from airports in prep_weather_daily
  SELECT                                -- (JFK, LAX, MIA)
    airport_code,
    date,
    EXTRACT(YEAR FROM date) AS year,
    cw,
    min_temp_c,
    max_temp_c,
    COALESCE(precipitation_mm, 0) AS precipitation_mm,  -- Replace NULLs with 0
    COALESCE(max_snow_mm, 0) AS max_snow_mm,            -- Replace NULLs with 0
    avg_wind_direction,
    avg_wind_speed_kmh,
    wind_peakgust_kmh
  FROM {{ ref('prep_weather_daily') }}
),
weekly_weather AS (                     -- Aggregate daily weather data to weekly stats...

  SELECT
    airport_code,
    year,
    cw,
    MIN(min_temp_c) AS weekly_min_temp_c,
    MAX(max_temp_c) AS weekly_max_temp_c,
    SUM(precipitation_mm) AS weekly_total_precipitation_mm,
    SUM(max_snow_mm) AS weekly_total_snow_mm,
    AVG(avg_wind_direction) AS weekly_avg_wind_direction,
    AVG(avg_wind_speed_kmh) AS weekly_avg_wind_speed_kmh,
    MAX(wind_peakgust_kmh) AS weekly_max_wind_peakgust_kmh
  FROM daily_weather
  GROUP BY airport_code, year, cw       -- ...by using GROUP BY year and calendar week
),
weekly_faa_stats_weather AS (
  SELECT                                  -- Final join to get the name, city, country from prep_airports
    w.airport_code,
    a.name,
    a.city,
    a.country,
    w.year,
    w.cw AS week,
    w.weekly_min_temp_c,
    w.weekly_max_temp_c,
    w.weekly_total_precipitation_mm,
    w.weekly_total_snow_mm,
    w.weekly_avg_wind_direction,
    w.weekly_avg_wind_speed_kmh,
    w.weekly_max_wind_peakgust_kmh
  FROM weekly_weather w
  LEFT JOIN {{ ref('prep_airports') }} a
    ON w.airport_code = a.faa
  ORDER BY w.year, w.cw, w.airport_code
)
SELECT * FROM weekly_faa_stats_weather