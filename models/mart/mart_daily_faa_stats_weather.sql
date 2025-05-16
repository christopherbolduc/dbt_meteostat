WITH weather_airports AS (              -- Limit to airports with available daily weather data
  SELECT DISTINCT airport_code
  FROM {{ ref('prep_weather_daily') }}  -- (only JFK, LAX, MIA weather data here)
),
flights_union AS (                      -- Stack ALL flights as events (either dept or arr)
  SELECT 
    origin AS airport_id,
    flight_date,
    TRUE AS is_departure,
    cancelled,
    diverted,
    tail_number,
    airline
  FROM {{ ref('prep_flights') }}
  UNION ALL
  SELECT 
    dest AS airport_id,
    flight_date,
    FALSE AS is_departure,
    cancelled,
    diverted,
    tail_number,
    airline
  FROM {{ ref('prep_flights') }}
),
filtered_flights AS (                   -- Filter only flights for airports with weather data
  SELECT f.*
  FROM flights_union f
  JOIN weather_airports w
    ON f.airport_id = w.airport_code
),
daily_airport_stats AS (                -- Aggregate flight metrics per airport and day
  SELECT
    airport_id,
    flight_date,
    COUNT(DISTINCT CASE WHEN is_departure THEN tail_number END) AS unique_departure_airplanes,
    COUNT(DISTINCT CASE WHEN NOT is_departure THEN tail_number END) AS unique_arrival_airplanes,
    COUNT(*) AS total_planned,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
    SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
    SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS total_occurred,
    COUNT(DISTINCT tail_number) AS unique_airplanes,
    COUNT(DISTINCT airline) AS unique_airlines
  FROM filtered_flights
  GROUP BY airport_id, flight_date
),
joined_with_weather AS (                -- Join flight stats with weather data
  SELECT
    das.airport_id,
    das.flight_date,
    --
    das.unique_departure_airplanes,
    das.unique_arrival_airplanes,
    das.total_planned,
    das.total_cancelled,
    das.total_diverted,
    das.total_occurred,
    das.unique_airplanes,
    das.unique_airlines,
    --
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh
  FROM daily_airport_stats das
  LEFT JOIN {{ ref('prep_weather_daily') }} w
    ON das.airport_id = w.airport_code AND das.flight_date = w.date
),
daily_faa_stats_weather AS (            -- Final join to get airport name, city, and country from prep_airports
  SELECT
    j.airport_id,
    a.name,
    a.city,
    a.country,
    j.flight_date,
    --
    j.unique_departure_airplanes,
    j.unique_arrival_airplanes,
    j.total_planned,
    j.total_cancelled,
    j.total_diverted,
    j.total_occurred,
    j.unique_airplanes,
    j.unique_airlines,
    --
    j.min_temp_c,
    j.max_temp_c,
    j.precipitation_mm,
    j.max_snow_mm,
    j.avg_wind_direction,
    j.avg_wind_speed_kmh,
    j.wind_peakgust_kmh
  FROM joined_with_weather j
  LEFT JOIN {{ ref('prep_airports') }} a
    ON j.airport_id = a.faa
)
SELECT * FROM daily_faa_stats_weather
