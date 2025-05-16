WITH flights_filtered AS (      -- Base flight data
  SELECT
    origin,
    dest,
    actual_elapsed_time,
    arr_delay,
    tail_number,
    airline,
    cancelled,
    diverted
  FROM {{ ref('prep_flights') }}
),
valid_flights AS (              -- Exclude cancelled/diverted flights
  SELECT *
  FROM flights_filtered
  WHERE cancelled = 0 AND diverted = 0
),
route_stats AS (                -- Aggregate stats for valid flights only
  SELECT
    origin,
    dest,
    COUNT(*)                        AS total_flights,
    COUNT(DISTINCT tail_number)     AS unique_airplanes,
    COUNT(DISTINCT airline)         AS unique_airlines,
    AVG(actual_elapsed_time)        AS avg_actual_elapsed_time,
    AVG(arr_delay)                  AS avg_arr_delay,
    MAX(arr_delay)                  AS max_arr_delay,
    MIN(arr_delay)                  AS min_arr_delay
  FROM valid_flights
  GROUP BY origin, dest
),
route_status_counts AS (        -- Count (sum up) all cancellations and diversions
  SELECT
    origin,
    dest,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
    SUM(CASE WHEN diverted  = 1 THEN 1 ELSE 0 END) AS total_diverted
  FROM flights_filtered
  GROUP BY origin, dest
),
final_route_stats AS (          -- Join airport metadata for both origin and destination airports
  SELECT
    rs.origin,
    a1.name      AS origin_name,
    a1.city      AS origin_city,
    a1.country   AS origin_country,
    --
    rs.dest,
    a2.name      AS dest_name,
    a2.city      AS dest_city,
    a2.country   AS dest_country,
    --
    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    --
    rs.avg_actual_elapsed_time,
    rs.avg_arr_delay,
    rs.max_arr_delay,
    rs.min_arr_delay,
    sc.total_cancelled,
    sc.total_diverted
  FROM route_stats rs
  LEFT JOIN route_status_counts sc
    ON rs.origin = sc.origin AND rs.dest = sc.dest
  LEFT JOIN {{ ref('prep_airports') }} a1
    ON rs.origin = a1.faa
  LEFT JOIN {{ ref('prep_airports') }} a2
    ON rs.dest = a2.faa
)
SELECT * FROM final_route_stats
