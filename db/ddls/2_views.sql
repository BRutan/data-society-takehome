-- a) How many biomass generators currently operable?
CREATE VIEW analysis.biomass_generators_count AS 
WITH latest AS (
  SELECT
    g.*,
    ROW_NUMBER() OVER (
      PARTITION BY g.plant_code, g.generator_id
      ORDER BY g.file_year DESC, g.file_date DESC
    ) AS rn
  FROM generator.operable g
)
SELECT COUNT(*) AS biomass_operable_generators 
FROM latest g
WHERE g.rn = 1
  AND (
    g.energy_source_1 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
    OR g.energy_source_2 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
    OR g.energy_source_3 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
    OR g.energy_source_4 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
    OR g.energy_source_5 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
    OR g.energy_source_6 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
  );

-- b) Which utilities could potentially offset existing biomass capacity
--  with existing renewables?
CREATE VIEW analysis.utilities_with_existing_renewable_offset AS 
WITH latest_biomass AS (
  SELECT *
  FROM (
    SELECT
      g.*,
      ROW_NUMBER() OVER (
        PARTITION BY g.plant_code, g.generator_id
        ORDER BY g.file_year DESC, g.file_date DESC
      ) AS rn
    FROM generator.operable g
    WHERE
      g.energy_source_1 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
      OR g.energy_source_2 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
      OR g.energy_source_3 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
      OR g.energy_source_4 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
      OR g.energy_source_5 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
      OR g.energy_source_6 IN ('AB','MSW','OBS','WDS','OBL','SLW','BLQ','WDL','LFG','OBG')
  ) x
  WHERE rn = 1
),
biomass AS (
  SELECT
    utility_id,
    utility_name,
    SUM(nameplate_capacity_mw) AS biomass_mw
  FROM latest_biomass
  GROUP BY utility_id, utility_name
),
latest_solar AS (
  SELECT *
  FROM (
    SELECT
      s.*,
      ROW_NUMBER() OVER (
        PARTITION BY s.plant_code, s.generator_id
        ORDER BY s.file_year DESC, s.file_date DESC
      ) AS rn
    FROM solar.operable s
  ) x
  WHERE rn = 1
),
latest_wind AS (
  SELECT *
  FROM (
    SELECT
      w.*,
      ROW_NUMBER() OVER (
        PARTITION BY w.plant_code, w.generator_id
        ORDER BY w.file_year DESC, w.file_date DESC
      ) AS rn
    FROM wind.operable w
  ) x
  WHERE rn = 1
),
latest_storage AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY e.plant_code, e.generator_id
        ORDER BY e.file_year DESC, e.file_date DESC
      ) AS rn
    FROM energystorage.operable e
  ) x
  WHERE rn = 1
),
renewables AS (
  SELECT utility_id, utility_name, SUM(nameplate_capacity_mw) AS renew_mw
  FROM (
    SELECT utility_id, utility_name, nameplate_capacity_mw FROM latest_solar
    UNION ALL
    SELECT utility_id, utility_name, nameplate_capacity_mw FROM latest_wind
    UNION ALL
    SELECT utility_id, utility_name, nameplate_capacity_mw FROM latest_storage
  ) r
  GROUP BY utility_id, utility_name
)
SELECT
  b.utility_id,
  b.utility_name,
  b.biomass_mw,
  COALESCE(r.renew_mw, 0) AS renew_mw,
  (COALESCE(r.renew_mw, 0) - b.biomass_mw) AS surplus_mw
FROM biomass b
LEFT JOIN renewables r
  ON r.utility_id = b.utility_id
WHERE COALESCE(r.renew_mw, 0) >= b.biomass_mw
ORDER BY surplus_mw DESC;

-- c) How many facilities of each type exist within each state?
CREATE VIEW analysis.facility_type_count_by_state AS 
WITH latest AS (
  SELECT *
  FROM (
    SELECT
      g.*,
      ROW_NUMBER() OVER (
        PARTITION BY plant_code
        ORDER BY file_year DESC, file_date DESC
      ) AS rn
    FROM generator.operable g
  ) x
  WHERE rn = 1
)
SELECT
  state,
  technology,
  COUNT(DISTINCT plant_code) AS facilities
FROM latest
GROUP BY state, technology
ORDER BY state, facilities DESC;

-- d) What percentage of utilities maintain solar facilities in multiple states?
CREATE VIEW analysis.multi_state_solar_facilities_percentage AS 
WITH utility_state_counts AS (
  SELECT
    utility_id,
    utility_name,
    COUNT(DISTINCT state) AS state_count
  FROM solar.operable
  GROUP BY utility_id, utility_name
),
summary AS (
  SELECT
    COUNT(*) AS total_utilities,
    COUNT(*) FILTER (WHERE state_count > 1) AS multistate_utilities
  FROM utility_state_counts
)
SELECT
  multistate_utilities,
  total_utilities,
  ROUND(
    100.0 * multistate_utilities / total_utilities,
    2
  ) AS percent_multistate
FROM summary;

-- e) Median battery capacity per year?
CREATE VIEW analysis.median_battery_capacity_by_year AS 
WITH latest AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY plant_code, generator_id, file_year
        ORDER BY file_date DESC
      ) AS rn
    FROM energystorage.operable e
    WHERE nameplate_capacity_mw IS NOT NULL
  ) x
  WHERE rn = 1
)
SELECT
  file_year,
  PERCENTILE_CONT(0.5)
    WITHIN GROUP (ORDER BY nameplate_capacity_mw) AS median_battery_mw
FROM latest
GROUP BY file_year
ORDER BY file_year;

-- f) YoY increase in nameplate capacity for wind + solar per utility?
CREATE VIEW analysis.wind_solar_capacity_by_utility AS
WITH utility_year_capacity AS (
  SELECT
    utility_id,
    utility_name,
    file_year,
    SUM(nameplate_capacity_mw) AS wind_solar_capacity_mw
  FROM (
    SELECT utility_id, utility_name, file_year, nameplate_capacity_mw
    FROM wind.operable
    WHERE nameplate_capacity_mw IS NOT NULL

    UNION ALL

    SELECT utility_id, utility_name, file_year, nameplate_capacity_mw
    FROM solar.operable
    WHERE nameplate_capacity_mw IS NOT NULL
  ) x
  GROUP BY utility_id, utility_name, file_year
)
SELECT
  utility_id,
  utility_name,
  file_year,
  wind_solar_capacity_mw,
  wind_solar_capacity_mw
    - LAG(wind_solar_capacity_mw) OVER (
        PARTITION BY utility_id
        ORDER BY file_year
      ) AS yoy_increase_mw
FROM utility_year_capacity
ORDER BY utility_id, file_year;


-- g) What is the current capacity mix by fuel type across a given region?
CREATE VIEW analysis.current_capacity_mix_by_fuel_type AS 
WITH latest_year AS (
  SELECT MAX(file_year) AS file_year
  FROM generator.operable
),
capacity AS (
  -- Thermal + general generators
  SELECT
    state,
    energy_source_1 AS fuel_type,
    nameplate_capacity_mw
  FROM generator.operable g
  JOIN latest_year y ON g.file_year = y.file_year
  WHERE nameplate_capacity_mw IS NOT NULL

  UNION ALL

  -- Solar
  SELECT
    state,
    'Solar' AS fuel_type,
    nameplate_capacity_mw
  FROM solar.operable s
  JOIN latest_year y ON s.file_year = y.file_year
  WHERE nameplate_capacity_mw IS NOT NULL

  UNION ALL

  -- Wind
  SELECT
    state,
    'Wind' AS fuel_type,
    nameplate_capacity_mw
  FROM wind.operable w
  JOIN latest_year y ON w.file_year = y.file_year
  WHERE nameplate_capacity_mw IS NOT NULL
)
SELECT
  state,
  fuel_type,
  SUM(nameplate_capacity_mw) AS capacity_mw,
  ROUND(
    100.0 * SUM(nameplate_capacity_mw)
    / SUM(SUM(nameplate_capacity_mw)) OVER (PARTITION BY state),
    2
  ) AS pct_of_state_capacity
FROM capacity
GROUP BY state, fuel_type
ORDER BY state, capacity_mw DESC;

-- h) Generators approaching retirement age?
CREATE VIEW analysis.generators_reaching_retirement AS 
WITH current_year AS (
  SELECT MAX(file_year) AS year
  FROM generator.operable
),
generator_age AS (
  SELECT
    g.utility_id,
    g.utility_name,
    g.plant_code,
    g.plant_name,
    g.generator_id,
    g.state,
    g.technology,
    g.energy_source_1,
    g.operating_year,
    c.year AS current_year,
    (c.year - g.operating_year) AS age_years,
    g.nameplate_capacity_mw
  FROM generator.operable g
  CROSS JOIN current_year c
  WHERE g.operating_year IS NOT NULL
)
SELECT
  utility_id,
  utility_name,
  plant_code,
  plant_name,
  generator_id,
  state,
  technology,
  energy_source_1,
  operating_year,
  age_years,
  nameplate_capacity_mw
FROM generator_age
WHERE age_years >= 40
ORDER BY age_years DESC, nameplate_capacity_mw DESC;
-- i) How is renewable energy (solar/wind) and battery storage penetration changing,
-- and where are the geographic locations where the amount of positive change
-- (increase) is greatest?
CREATE OR REPLACE VIEW analysis.capacity_by_tech_geo_year AS
WITH
-- total operable capacity (denominator)
total AS (
  SELECT
    file_year,
    state,
    county,
    SUM(nameplate_capacity_mw) AS total_mw
  FROM generator.operable
  GROUP BY file_year, state, county
),

solar AS (
  SELECT
    file_year,
    state,
    county,
    SUM(nameplate_capacity_mw) AS solar_mw
  FROM solar.operable
  GROUP BY file_year, state, county
),

wind AS (
  SELECT
    file_year,
    state,
    county,
    SUM(nameplate_capacity_mw) AS wind_mw
  FROM wind.operable
  GROUP BY file_year, state, county
),

storage AS (
  SELECT
    file_year,
    state,
    county,
    SUM(nameplate_capacity_mw) AS storage_mw
  FROM energystorage.operable
  GROUP BY file_year, state, county
)

SELECT
  t.file_year,
  t.state,
  t.county,
  t.total_mw,
  COALESCE(s.solar_mw, 0)   AS solar_mw,
  COALESCE(w.wind_mw, 0)    AS wind_mw,
  COALESCE(es.storage_mw, 0) AS storage_mw
FROM total t
LEFT JOIN solar s
  ON s.file_year=t.file_year AND s.state=t.state AND s.county=t.county
LEFT JOIN wind w
  ON w.file_year=t.file_year AND w.state=t.state AND w.county=t.county
LEFT JOIN storage es
  ON es.file_year=t.file_year AND es.state=t.state AND es.county=t.county;
  
-- j) Relationship between planned/proposed capacity additions 
-- & recent retirements?
CREATE VIEW analysis.additions_vs_retirements_geo AS
WITH params AS (
  SELECT
    (SELECT MAX(file_year) FROM generator.operable) AS max_year,
    3::int AS recent_years
),
-- -----------------------------
-- Recent retirements (MW)
-- -----------------------------
ret_gen AS (
  SELECT
    g.state,
    g.county,
    g.retirement_year::int AS year,
    SUM(g.nameplate_capacity_mw) AS retired_gen_mw,
    -- Optional useful splits (only if energy_source_1 is populated)
    SUM(CASE WHEN upper(COALESCE(g.energy_source_1,'')) IN ('SUN','WND','WAT','GEO','BIO','WAS','MSW') THEN g.nameplate_capacity_mw ELSE 0 END) AS retired_renew_mw,
    SUM(CASE WHEN upper(COALESCE(g.energy_source_1,'')) IN ('COL','BIT','SUB','LIG','NG','DFO','RFO','OIL','PC','NGO','BFG','OG','KER','JF','PG','SGC') THEN g.nameplate_capacity_mw ELSE 0 END) AS retired_fossil_mw
  FROM generator.retired_and_canceled g
  JOIN params p ON TRUE
  WHERE g.retirement_year IS NOT NULL
    AND g.retirement_year ~ '^\d{4}$'
    AND g.retirement_year::int >= (p.max_year - p.recent_years)
    AND g.retirement_year::int <= p.max_year
  GROUP BY g.state, g.county, g.retirement_year::int
),
ret_solar AS (
  SELECT
    s.state,
    s.county,
    s.retirement_year::int AS year,
    SUM(s.nameplate_capacity_mw) AS retired_solar_mw
  FROM solar.retired_and_canceled s
  JOIN params p ON TRUE
  WHERE s.retirement_year IS NOT NULL
    AND s.retirement_year ~ '^\d{4}$'
    AND s.retirement_year::int >= (p.max_year - p.recent_years)
    AND s.retirement_year::int <= p.max_year
  GROUP BY s.state, s.county, s.retirement_year::int
),
ret_wind AS (
  SELECT
    w.state,
    w.county,
    w.retirement_year::int AS year,
    SUM(w.nameplate_capacity_mw) AS retired_wind_mw
  FROM wind.retired_and_canceled w
  JOIN params p ON TRUE
  WHERE w.retirement_year IS NOT NULL
    AND w.retirement_year ~ '^\d{4}$'
    AND w.retirement_year::int >= (p.max_year - p.recent_years)
    AND w.retirement_year::int <= p.max_year
  GROUP BY w.state, w.county, w.retirement_year::int
),
ret_storage AS (
  SELECT
    e.state,
    e.county,
    e.retirement_year::int AS year,
    SUM(e.nameplate_capacity_mw) AS retired_storage_mw
  FROM energystorage.retired_and_canceled e
  JOIN params p ON TRUE
  WHERE e.retirement_year IS NOT NULL
    AND e.retirement_year ~ '^\d{4}$'
    AND e.retirement_year::int >= (p.max_year - p.recent_years)
    AND e.retirement_year::int <= p.max_year
  GROUP BY e.state, e.county, e.retirement_year::int
),
ret_all AS (
  SELECT
    state,
    county,
    year,
    COALESCE(SUM(retired_gen_mw),0)     AS retired_gen_mw,
    COALESCE(SUM(retired_renew_mw),0)   AS retired_renew_mw,
    COALESCE(SUM(retired_fossil_mw),0)  AS retired_fossil_mw,
    0::numeric AS retired_solar_mw,
    0::numeric AS retired_wind_mw,
    0::numeric AS retired_storage_mw
  FROM ret_gen
  GROUP BY state, county, year

  UNION ALL
  SELECT state, county, year,
    0, 0, 0,
    COALESCE(SUM(retired_solar_mw),0), 0, 0
  FROM ret_solar
  GROUP BY state, county, year

  UNION ALL
  SELECT state, county, year,
    0, 0, 0,
    0, COALESCE(SUM(retired_wind_mw),0), 0
  FROM ret_wind
  GROUP BY state, county, year

  UNION ALL
  SELECT state, county, year,
    0, 0, 0,
    0, 0, COALESCE(SUM(retired_storage_mw),0)
  FROM ret_storage
  GROUP BY state, county, year
),
ret_rollup AS (
  SELECT
    state,
    county,
    SUM(retired_gen_mw)     AS retired_gen_mw_recent,
    SUM(retired_renew_mw)   AS retired_renew_mw_recent,
    SUM(retired_fossil_mw)  AS retired_fossil_mw_recent,
    SUM(retired_solar_mw)   AS retired_solar_mw_recent,
    SUM(retired_wind_mw)    AS retired_wind_mw_recent,
    SUM(retired_storage_mw) AS retired_storage_mw_recent
  FROM ret_all
  GROUP BY state, county
),

-- -----------------------------
-- Proposed additions (MW)
-- -----------------------------
prop_gen AS (
  SELECT
    g.state,
    g.county,
    g.current_year::int AS year,
    SUM(g.nameplate_capacity_mw) AS proposed_gen_mw
  FROM generator.proposed g
  JOIN params p ON TRUE
  WHERE g.current_year IS NOT NULL
    AND g.current_year >= (p.max_year - p.recent_years)
    AND g.current_year <= p.max_year
  GROUP BY g.state, g.county, g.current_year::int
),
prop_storage AS (
  SELECT
    e.state,
    e.county,
    e.current_year::int AS year,
    SUM(e.nameplate_capacity_mw) AS proposed_storage_mw
  FROM energystorage.proposed e
  JOIN params p ON TRUE
  WHERE e.current_year IS NOT NULL
    AND e.current_year >= (p.max_year - p.recent_years)
    AND e.current_year <= p.max_year
  GROUP BY e.state, e.county, e.current_year::int
),
-- Note: you don't have solar.proposed / wind.proposed tables in your DDL.
-- If solar/wind proposals are stored in generator.proposed via energy_source_1,
-- you can derive them there; otherwise omit.
prop_rollup AS (
  SELECT
    state,
    county,
    SUM(proposed_gen_mw) AS proposed_gen_mw_recent
  FROM prop_gen
  GROUP BY state, county
),
prop_storage_rollup AS (
  SELECT
    state,
    county,
    SUM(proposed_storage_mw) AS proposed_storage_mw_recent
  FROM prop_storage
  GROUP BY state, county
)

SELECT
  COALESCE(r.state, p.state, ps.state)   AS state,
  COALESCE(r.county, p.county, ps.county) AS county,

  -- Recent retirements
  COALESCE(r.retired_gen_mw_recent, 0)     AS retired_gen_mw_recent,
  COALESCE(r.retired_fossil_mw_recent, 0)  AS retired_fossil_mw_recent,
  COALESCE(r.retired_renew_mw_recent, 0)   AS retired_renew_mw_recent,
  COALESCE(r.retired_solar_mw_recent, 0)   AS retired_solar_mw_recent,
  COALESCE(r.retired_wind_mw_recent, 0)    AS retired_wind_mw_recent,
  COALESCE(r.retired_storage_mw_recent, 0) AS retired_storage_mw_recent,

  -- Proposed additions
  COALESCE(p.proposed_gen_mw_recent, 0)    AS proposed_gen_mw_recent,
  COALESCE(ps.proposed_storage_mw_recent, 0) AS proposed_storage_mw_recent,

  -- Combined proposed (generator proposals + storage proposals)
  (COALESCE(p.proposed_gen_mw_recent, 0) + COALESCE(ps.proposed_storage_mw_recent, 0)) AS proposed_total_mw_recent,

  -- Net change vs retirements (using generator retirements as the retirement baseline)
  (COALESCE(p.proposed_gen_mw_recent, 0) + COALESCE(ps.proposed_storage_mw_recent, 0))
  - COALESCE(r.retired_gen_mw_recent, 0) AS net_mw_change_recent,

  -- Replacement ratio
  (COALESCE(p.proposed_gen_mw_recent, 0) + COALESCE(ps.proposed_storage_mw_recent, 0))
  / NULLIF(COALESCE(r.retired_gen_mw_recent, 0), 0) AS replacement_ratio_recent

FROM ret_rollup r
FULL OUTER JOIN prop_rollup p
  ON r.state = p.state AND r.county = p.county
FULL OUTER JOIN prop_storage_rollup ps
  ON COALESCE(r.state, p.state) = ps.state
 AND COALESCE(r.county, p.county) = ps.county;