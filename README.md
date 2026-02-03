# data-society takehome

## 0) Prerequisites:
- docker

## 1) Setup:
- Create an .env file in same folder as docker-compose.yml with the following:
```console
POSTGRES_PASSWORD=<choose a password>
```
- Run the following commands to build and run the db and etl containers.
```console
docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml up -d
```

## 2) ETL:
Run the following jobs **in order** to generate schemas, metadata, and load raw EIA data.
---
### 1. `jobs/gen_ddls.py` — DDL & Metadata Generation
This job performs the following steps:
1. **Download & extract source data**
   - Fetches all requested yearly `.zip` files from the EIA website.
2. **Infer PostgreSQL schemas**
   - Uses **Polars** to analyze column values and infer PostgreSQL-compatible DDLs.
   - Chooses the **most conservative data type**:
     - Prefers numeric types when possible
     - Falls back to text when data is mixed or contains ASCII characters
3. **Generate metadata artifacts (JSON)**
   - `file_table_map.json`  
     Maps standardized `{ file_name → sheet_name → table_name }`
   - `table_schemas.json`  
     Final PostgreSQL column metadata per table
   - `table_years.json`  
     List of years for which each table has data (based on file presence)
   - `year_schemas.json`  
     Column-level schema metadata by year (used to derive final DDLs)
4. **Generate PostgreSQL DDLs**
   - Outputs `table_ddls.sql` containing raw `CREATE TABLE` statements derived from the consolidated column metadata
### Run command
```console
docker exec data-society-etl bash -c '. ~/.zshrc && cd $HOME && python jobs/gen_ddls.py --start_year 2019 --end_year 2024'
```

### `jobs/pull_load_raw_data.py` — Raw Data Ingestion & Load
This job is responsible for ingesting raw EIA source data and loading it into the database.
1. **Download & extract source data**
   - Fetches all requested yearly `.zip` files from the EIA website.
   - Extracts all contained files for downstream processing.
2. **Load data into raw tables**
   - Loads each extracted file into its corresponding target **raw** PostgreSQL table.
   - Applies basic transformations as needed, including:
     - Column name standardization
     - Type coercion for compatibility
     - Handling nulls and malformed values
---
### Run commands
```console
docker exec data-society-etl bash -c '. ~/.zshrc && cd $HOME && python jobs/pull_load_raw_data.py --start_year 2019 --end_year 2024'
```

## 3. Analytical Views

The following views answer business questions **(a)–(j)** using the curated EIA dataset.  
Each command can be run directly against the PostgreSQL container to inspect the results.

---

### a) Biomass Generators

**How many biomass generators are currently operable?**

```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.biomass_generators_count;"'
```

### b) Renewable Offset Potential
**Which utilities could potentially offset existing biomass capacity with existing renewable resources?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.utilities_with_existing_renewable_offset;"'
```
### c) Facility Distribution by State
**How many facilities of each type (generator, solar, wind, storage, etc.) exist within each state?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.facility_type_count_by_state;"'
```

### d) Multi-State Solar Operations
**What percentage of utilities maintain solar facilities in multiple states?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.multi_state_solar_facilities_percentage;"'
```

### e) Battery Storage Trends
**What is the median battery storage capacity per year?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.median_battery_capacity_by_year;"'
```

### f) Wind & Solar Growth
**What is the year-over-year increase in nameplate capacity for wind and solar, by utility?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.wind_solar_capacity_by_utility;"'
```

### g) Capacity Mix Evolution
**What is the current capacity mix by fuel type across a given region, and how has it evolved over the past five years?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.current_capacity_mix_by_fuel_type;"'
```

### h) Generator Retirement Risk
**Which generators are approaching retirement age (>40 years), and what potential capacity gap would their retirement create?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.generators_reaching_retirement;"'
```

### i) Renewable & Storage Penetration
**How are renewable energy (solar/wind) and battery storage penetration changing, and where are the geographic areas with the greatest increases?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.capacity_by_tech_geo_year;"'
```

### j) Additions vs. Retirements
**What is the relationship between planned/proposed capacity additions and recent retirements?**
```console
docker exec data-society-db bash -c 'psql -d eia_data -c "SELECT * FROM analysis.additions_vs_retirements_geo;"'
```

## 4) Discussion:
Given more time, I would perform the following:

1. Investigate the relationships between the data thoroughly and institute a star schema. 
2. Map standard codes to a separate table for visibility purposes, and check consistency across years.
3. Implement indexes on columns used in join predicates, partition by and group by clauses within the views to improve performance.

In any workplace setting I would not expose raw credentials in the docker-compose.yml.