# Data Engineering and Warehousing (Group Work)

This project demonstrates a **three-layer data warehouse architecture** implemented with a practical **ETL pipeline** using:
- **PostgreSQL** (running in Docker)
- A Python ETL runner (`etl_pipeline.py`)
- Two provided CSV datasets:
  - `occupazione.csv` (employment rates)
  - `disoccupazione.csv` (unemployment rates)

The warehouse layers are implemented as PostgreSQL schemas:
- `ingestion` = raw landing data (TEXT)
- `staging` = validated + typed/standardized data
- `mart` = joined and analytics-ready data (fact-like tables + derived metrics)

---

## 1. Datasets

Both CSV files share the same schema:
- `iso_code` (string)
- `country` (string)
- `sex` (`Male`, `Female`, `Total`)
- `age` (`15+`, `15-24`, `25+`)
- `year` (int-like)
- `obs_value` (numeric-like, expressed as a percentage)

Files:
- `occupazione.csv` → **employment_rate**
- `disoccupazione.csv` → **unemployment_rate**

### Dataset assumptions

Because the dataset contains **rates (%):**
- Values must be numeric
- Values are expected to be between **0 and 100** (inclusive)
- Valid dimensional members are limited to:
  - `sex IN ('Male', 'Female', 'Total')`
  - `age IN ('15+', '15-24', '25+')`
  - `year` should be in the range **1991–2025**

---

## 2. Architecture Overview (Three Layers)

### 2.1 Ingestion Layer (`ingestion` schema)

Role:
- Store the incoming CSV rows as-is (as **TEXT**) to preserve traceability.

Tables:
- `ingestion.occupazione_raw`
- `ingestion.disoccupazione_raw`

Characteristics:
- No business logic applied besides storing the data
- Allows later reproducibility and debugging of parsing issues

### 2.2 Staging Layer (`staging` schema)

Role:
- Validate data quality
- Standardize values
- Convert to correct data types for reliable analytics joins

Tables:
- `staging.occupazione_stg`
- `staging.disoccupazione_stg`

Transformations:
- `year` cast to `INT`
- `obs_value` cast to `NUMERIC(10,3)`
- Create derived typed attributes:
  - `employment_rate` from `obs_value` (employment)
  - `unemployment_rate` from `obs_value` (unemployment)

Data quality filters:
- `year` regex check + range check (1991–2025)
- `sex` and `age` allowed sets
- `obs_value` numeric pattern + range check (0–100)

Idempotency handling:
- Inserts use:
  - `ON CONFLICT (iso_code, sex, age, year) DO NOTHING`
- This prevents duplicate-key failures even if reruns behave unexpectedly.

### 2.3 Mart Layer (`mart` schema)

Role:
- Provide analytics-ready tables that are already joined and include derived metrics.

Tables:
- `mart.country_year_sex_age_rates`
  - Joined dataset on `(iso_code, sex, age, year)`
  - Contains:
    - `employment_rate`
    - `unemployment_rate`
    - `unemployment_to_employment_ratio`
    - `unemployment_minus_employment`
- `mart.country_year_total_15plus`
  - Convenience slice for:
    - `sex='Total'` and `age='15+'`

Derived metrics:
- `unemployment_to_employment_ratio`
  - `(unemployment_rate / NULLIF(employment_rate, 0))`
- `unemployment_minus_employment`
  - `(unemployment_rate - employment_rate)`

Idempotency handling:
- Mart inserts use:
  - `ON CONFLICT ... DO UPDATE`
- This ensures repeated ETL runs keep mart content consistent.

---

## 3. Project Files

- `etl_pipeline.py`
  - Main ETL implementation (creates schemas, tables, loads CSVs, transforms, builds mart)
- `docker-compose.yml`
  - Starts PostgreSQL (`postgres`) and provides an `etl` one-shot runner service
- `Dockerfile`
  - Builds the Python ETL container image with CSV + ETL code inside
- `demo_mart_queries.sql`
  - A DBeaver-friendly SQL script with demo queries (counts, trends, top-10 rankings)

Datasets:
- `occupazione.csv`
- `disoccupazione.csv`

---

## 4. Prerequisites

### 4.1 Install
- Docker Desktop (or docker engine compatible with `docker compose`)
- Python is not required locally (ETL runs in Docker), but Docker must be able to build and run containers.
- DBeaver (for validating results)

### 4.2 Ports
The project maps PostgreSQL as:
- Host port: `5433` → Container port: `5432`

This avoids conflicts if your host already uses port `5432`.

---

## 5. Running the ETL

All commands assume your current directory is:
- `D:\Data-Engineering-and-warehousing`

### 5.1 Run only the ETL (Postgres must be available)
Run:
```powershell
docker compose up --build --abort-on-container-exit etl
```

If you already have the ETL image built, you can run without rebuilding:
```powershell
docker compose up --abort-on-container-exit etl
```

### 5.2 Start only PostgreSQL (optional)
```powershell
docker compose up -d postgres
```

### 5.3 Common workflow
1) Start PostgreSQL:
```powershell
docker compose up -d postgres
```
2) Run ETL:
```powershell
docker compose up --abort-on-container-exit etl
```

---

## 6. Idempotency Guarantees (Important)

This ETL is designed to be safe to re-run:
- Each ETL run does a deterministic refresh:
  - `TRUNCATE` all warehouse layer tables inside a transaction
  - reload ingestion from CSVs
  - rebuild staging and mart derived tables
- Staging and mart inserts also include `ON CONFLICT` clauses:
  - `staging`: DO NOTHING on natural keys
  - `mart`: DO UPDATE on natural keys

Result:
- You can re-run ETL for debugging without schema/data drift.

---

## 7. Validating Results in DBeaver

### 7.1 Connect to Postgres in DBeaver
Connection settings:
- Host: `localhost` (or `127.0.0.1`)
- Port: `5433`
- Database: `de_wh`
- Username: `de_user`
- Password: `de_pass`

Then expand schemas to verify:
- `ingestion`
- `staging`
- `mart`

### 7.2 Quick validation queries
Run:
```sql
SELECT COUNT(*) FROM mart.country_year_total_15plus;
SELECT COUNT(*) FROM mart.country_year_sex_age_rates;
```

If counts exist and match expected ranges, the pipeline is working.

### 7.3 Demo script
Run:
- `demo_mart_queries.sql`

This script includes:
- sanity counts
- record-per-year counts
- trend averages across years
- multiple top-10 rankings for the latest year
- an example time-series for Afghanistan (`AFG`)

---

## 8. Troubleshooting

### 8.1 Port already allocated
If you previously started another Postgres on host port `5432`, the project uses `5433`.
Make sure DBeaver uses port `5433`.

### 8.2 ETL fails due to Postgres readiness
The ETL runner includes retry logic (`db_connect_retries`) so temporary startup delays should not break the run.

### 8.3 Data quality filtering (unexpected low counts)
If your staged row counts are lower than expected:
- check the CSV contents
- confirm values in `obs_value` are numeric and in the range `0–100`
- confirm `sex` and `age` match allowed values

---

## 9. Suggested Screenshot/Demo Plan 

Recommended captures:
1. Screenshot showing the warehouse schemas:
   - `ingestion`, `staging`, `mart`
2. Screenshot showing validation counts from mart:
   - total 15+ slice
   - main joined fact table
3. Screenshot showing one top-10 ranking query result
4. Screenshot showing one trend query result (averages over years)

