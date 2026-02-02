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
WHERE age_years >= 35
ORDER BY age_years DESC, nameplate_capacity_mw DESC;
-- i) How is renewable energy and battery storage changing

-- j) Relationship between planned/proposed capacity additions 
-- & recent retirements?

