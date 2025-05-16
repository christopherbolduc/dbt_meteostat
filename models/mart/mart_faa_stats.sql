WITH airport_events AS (        -- Stack flights as events (either dept or arr)
    SELECT
      origin AS airport_id,
      TRUE AS is_departure,     -- Flag this row as a departure (vs arrival)
      cancelled,
      diverted,
      tail_number,
      airline
    FROM {{ ref('prep_flights') }}
    UNION ALL                   -- Stacked here
    SELECT
      dest AS airport_id,
      FALSE AS is_departure,    -- This can't be 'TRUE AS is_arrival' b/c UNION ALL
      cancelled,                -- requires exact same column structure & order
      diverted,
      tail_number,
      airline
    FROM {{ ref('prep_flights') }}
  ),
airport_stats AS (              -- Aggregations
    SELECT
      airport_id,
      SUM(CASE WHEN is_departure THEN 1 ELSE 0 END)                     AS total_planned_departures,
      SUM(CASE WHEN not is_departure THEN 1 ELSE 0 END)                 AS total_planned_arrivals,
      COUNT(*)                                                          AS total_planned,
      SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)                    AS total_cancelled,
      SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)                     AS total_diverted,
      SUM(CASE WHEN cancelled = 0 and diverted = 0 THEN 1 ELSE 0 END)   AS total_occurred,
      COUNT(DISTINCT tail_number)                                       AS unique_airplanes,
      COUNT(DISTINCT airline)                                           AS unique_airlines
    FROM airport_events
    GROUP BY airport_id
  )
SELECT
  a.faa,
  a.name,
  a.city,
  a.country,
  s.total_planned_departures,
  s.total_planned_arrivals,
  s.total_planned,
  s.total_cancelled,
  s.total_diverted,
  s.total_occurred,
  s.unique_airplanes,
  s.unique_airlines
FROM airport_stats s
JOIN {{ ref('prep_airports') }} a
  ON s.airport_id = a.faa
--WHERE s.airport_id in ('LAX','JFK','MIA')