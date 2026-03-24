import os
import sys
import time
import json
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import psycopg2  
from psycopg2.extensions import connection as PGConnection  


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    occupazione_csv: str
    disoccupazione_csv: str

    api_url: str = "https://www.apicountries.com/countries"
    api_timeout_seconds: int = 20
    api_fetch_retries: int = 3

    # Schemas implementing the three-layer architecture
    ingestion_schema: str = "ingestion"
    staging_schema: str = "staging"
    mart_schema: str = "mart"

    # Safety: retries when Postgres is not ready yet
    db_connect_retries: int = 20
    db_connect_retry_sleep_seconds: float = 2.0


def getenv_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    return Settings(
        db_host=getenv_required("DB_HOST"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=getenv_required("DB_NAME"),
        db_user=getenv_required("DB_USER"),
        db_password=getenv_required("DB_PASSWORD"),
        occupazione_csv=os.getenv("OCCUPAZIONE_CSV", "/app/Dataset/occupazione.csv"),
        disoccupazione_csv=os.getenv("DISOCCUPAZIONE_CSV", "/app/Dataset/disoccupazione.csv"),
        api_url=os.getenv("API_URL", "https://www.apicountries.com/countries"),
        api_timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "20")),
        api_fetch_retries=int(os.getenv("API_FETCH_RETRIES", "3")),
    )


def connect_with_retry(settings: Settings) -> PGConnection:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, settings.db_connect_retries + 1):
        try:
            return psycopg2.connect(
                host=settings.db_host,
                port=settings.db_port,
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
            )
        except Exception as exc:
            last_exc = exc
            print(
                f"[WARN] Postgres not ready yet (attempt {attempt}/{settings.db_connect_retries}). "
                f"Sleeping {settings.db_connect_retry_sleep_seconds}s...",
                file=sys.stderr,
            )
            time.sleep(settings.db_connect_retry_sleep_seconds)
    raise RuntimeError(f"Could not connect to Postgres after retries: {last_exc!r}")


def exec_sql(conn: PGConnection, sql: str, params: Optional[Sequence] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)


def fetch_one_value(conn: PGConnection, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        return 0
    return int(row[0])


def fetch_two_values(conn: PGConnection, sql: str) -> Tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        return 0, 0
    left = 0 if row[0] is None else int(row[0])
    right = 0 if row[1] is None else int(row[1])
    return left, right


def ensure_schemas(conn: PGConnection, schemas: Sequence[str]) -> None:
    for schema in schemas:
        exec_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {schema};")


def ensure_tables(conn: PGConnection, settings: Settings) -> None:
    """
    Create layer tables in an idempotent way.

    Three-layer architecture mapping:
    - ingestion: raw landing tables, keep values as TEXT (only COPY/load concerns here)
    - staging: validated + typed tables (cleaning, casting, constraints)
    - mart: analytics-ready tables (joins and derived metrics)
    """
    # Ingestion: raw text landing tables; no casting/validation here beyond COPY mechanics.
    exec_sql(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {settings.ingestion_schema}.occupazione_raw (
            iso_code TEXT,
            country TEXT,
            sex TEXT,
            age TEXT,
            year TEXT,
            obs_value TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {settings.ingestion_schema}.disoccupazione_raw (
            iso_code TEXT,
            country TEXT,
            sex TEXT,
            age TEXT,
            year TEXT,
            obs_value TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );

        -- Population API raw landing table.
        -- Source JSON fields (example):
        --   alpha3Code (-> iso_code), name (-> country), population (-> population)
        CREATE TABLE IF NOT EXISTS {settings.ingestion_schema}.population_raw (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            population BIGINT NOT NULL,
            api_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code)
        );
        """,
    )

    # Staging: typed + validated representations of the data.
    exec_sql(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {settings.staging_schema}.occupazione_stg (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            sex TEXT NOT NULL,
            age TEXT NOT NULL,
            year INT NOT NULL,
            employment_rate NUMERIC(10, 3) NOT NULL,
            etl_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code, sex, age, year)
        );

        CREATE TABLE IF NOT EXISTS {settings.staging_schema}.disoccupazione_stg (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            sex TEXT NOT NULL,
            age TEXT NOT NULL,
            year INT NOT NULL,
            unemployment_rate NUMERIC(10, 3) NOT NULL,
            etl_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code, sex, age, year)
        );

        -- Population API staging:
        -- Converts population to a typed numeric type for joins/derived metrics.
        CREATE TABLE IF NOT EXISTS {settings.staging_schema}.population_stg (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            population BIGINT NOT NULL,
            etl_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code)
        );
        """,
    )

    # Mart: analytics-ready joined dataset (+ a couple of useful slices).
    exec_sql(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {settings.mart_schema}.country_year_sex_age_rates (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            sex TEXT NOT NULL,
            age TEXT NOT NULL,
            year INT NOT NULL,

            employment_rate NUMERIC(10, 3) NOT NULL,
            unemployment_rate NUMERIC(10, 3) NOT NULL,

            unemployment_to_employment_ratio NUMERIC(12, 6),
            unemployment_minus_employment NUMERIC(10, 3),

            etl_loaded_at TIMESTAMP DEFAULT NOW(),

            UNIQUE (iso_code, sex, age, year)
        );

        CREATE TABLE IF NOT EXISTS {settings.mart_schema}.country_year_total_15plus (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            year INT NOT NULL,

            employment_rate_15plus_total NUMERIC(10, 3) NOT NULL,
            unemployment_rate_15plus_total NUMERIC(10, 3) NOT NULL,

            unemployment_to_employment_ratio NUMERIC(12, 6),
            unemployment_minus_employment NUMERIC(10, 3),

            etl_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code, year)
        );

        -- Population dimension-like snapshot stored in the mart for analysis.
        CREATE TABLE IF NOT EXISTS {settings.mart_schema}.country_population (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            population BIGINT NOT NULL,
            snapshot_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code)
        );

        -- Joined mart slice to compare unemployment/employment rates against population.
        -- Estimates absolute counts using: persons = population * rate / 100.
        CREATE TABLE IF NOT EXISTS {settings.mart_schema}.country_year_total_15plus_with_population (
            iso_code TEXT NOT NULL,
            country TEXT NOT NULL,
            year INT NOT NULL,
            population BIGINT NOT NULL,

            employment_rate_15plus_total NUMERIC(10, 3) NOT NULL,
            unemployment_rate_15plus_total NUMERIC(10, 3) NOT NULL,

            unemployment_to_employment_ratio NUMERIC(12, 6),
            unemployment_minus_employment NUMERIC(10, 3),

            employment_persons_est NUMERIC(22, 2),
            unemployment_persons_est NUMERIC(22, 2),

            etl_loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (iso_code, year)
        );

        -- Source-level run audit table to detect row/content changes between ETL runs.
        CREATE TABLE IF NOT EXISTS {settings.mart_schema}.etl_source_audit (
            source_name TEXT NOT NULL,
            observed_rows BIGINT NOT NULL,
            observed_fingerprint BIGINT NOT NULL,
            previous_rows BIGINT,
            previous_fingerprint BIGINT,
            delta_rows BIGINT,
            new_rows BIGINT,
            removed_rows BIGINT,
            change_type TEXT NOT NULL,
            observed_at TIMESTAMP DEFAULT NOW()
        );
        """,
    )


def truncate_tables(conn: PGConnection, settings: Settings) -> None:
    """
    Idempotency strategy:
    - Each ETL run recreates the dataset deterministically by reloading data.
    - We do this by TRUNCATE-ing all layer tables inside a single transaction.
    - Result rows (except timestamps like `loaded_at` / `etl_loaded_at`) are deterministic for the given CSVs.
    """
    exec_sql(conn, f"TRUNCATE TABLE {settings.ingestion_schema}.occupazione_raw;")
    exec_sql(conn, f"TRUNCATE TABLE {settings.ingestion_schema}.disoccupazione_raw;")

    exec_sql(conn, f"TRUNCATE TABLE {settings.staging_schema}.occupazione_stg;")
    exec_sql(conn, f"TRUNCATE TABLE {settings.staging_schema}.disoccupazione_stg;")

    exec_sql(conn, f"TRUNCATE TABLE {settings.mart_schema}.country_year_sex_age_rates;")
    exec_sql(conn, f"TRUNCATE TABLE {settings.mart_schema}.country_year_total_15plus;")

    exec_sql(conn, f"TRUNCATE TABLE {settings.ingestion_schema}.population_raw;")
    exec_sql(conn, f"TRUNCATE TABLE {settings.staging_schema}.population_stg;")
    exec_sql(conn, f"TRUNCATE TABLE {settings.mart_schema}.country_population;")
    exec_sql(conn, f"TRUNCATE TABLE {settings.mart_schema}.country_year_total_15plus_with_population;")


def copy_csv_to_table(
    conn: PGConnection,
    schema: str,
    table: str,
    columns: Sequence[str],
    csv_path: str,
) -> None:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found at path: {csv_path}")

    column_list = ", ".join(columns)
    copy_sql = (
        f"COPY {schema}.{table} ({column_list}) "
        f"FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    with conn.cursor() as cur:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            cur.copy_expert(copy_sql, f)


def seed_ingestion(conn: PGConnection, settings: Settings) -> None:
    """
    Ingestion layer implementation:
    - Source of truth for this demo: the two provided CSV files.
    - We load both CSVs into `ingestion.*_raw` landing tables via `COPY ... FROM STDIN`.
    - No transformations are applied at this step (values stored as TEXT for traceability).
    """
    # COPY into raw ingestion tables (all values as text).
    cols = ["iso_code", "country", "sex", "age", "year", "obs_value"]
    copy_csv_to_table(
        conn,
        settings.ingestion_schema,
        "occupazione_raw",
        cols,
        settings.occupazione_csv,
    )
    copy_csv_to_table(
        conn,
        settings.ingestion_schema,
        "disoccupazione_raw",
        cols,
        settings.disoccupazione_csv,
    )


def seed_staging(conn: PGConnection, settings: Settings) -> None:
    """
    Staging layer implementation:
    - Validates record shape (year/sex/age allowed values).
    - Validates numeric bounds since rates are expressed as percentages.
    - Casts columns into typed schema for downstream consistency:
        - employment_rate, unemployment_rate -> NUMERIC(10,3)
    - Resilience/idempotency:
        - Even with TRUNCATE, the same run could encounter duplicates coming from the CSV.
        - `ON CONFLICT DO NOTHING` prevents the run from failing due to duplicate keys.
    """
    # Staging validation/casting rules:
    # - year must be 4 digits and within [1991, 2025]
    # - sex in allowed set
    # - age in allowed set
    # - obs_value numeric and within [0, 100] since these are rates (%)
    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.staging_schema}.occupazione_stg
            (iso_code, country, sex, age, year, employment_rate)
        SELECT
            iso_code,
            country,
            sex,
            age,
            year::int AS year,
            obs_value::numeric(10,3) AS employment_rate
        FROM {settings.ingestion_schema}.occupazione_raw
        WHERE
            year ~ '^[0-9]{{4}}$'
            AND (year::int BETWEEN 1991 AND 2025)
            AND sex IN ('Male', 'Female', 'Total')
            AND age IN ('15+', '15-24', '25+')
            AND obs_value ~ '^([0-9]+)(\\.[0-9]+)?$'
            AND (obs_value::numeric(10,3) BETWEEN 0 AND 100)
        ON CONFLICT (iso_code, sex, age, year) DO NOTHING;
        """,
    )

    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.staging_schema}.disoccupazione_stg
            (iso_code, country, sex, age, year, unemployment_rate)
        SELECT
            iso_code,
            country,
            sex,
            age,
            year::int AS year,
            obs_value::numeric(10,3) AS unemployment_rate
        FROM {settings.ingestion_schema}.disoccupazione_raw
        WHERE
            year ~ '^[0-9]{{4}}$'
            AND (year::int BETWEEN 1991 AND 2025)
            AND sex IN ('Male', 'Female', 'Total')
            AND age IN ('15+', '15-24', '25+')
            AND obs_value ~ '^([0-9]+)(\\.[0-9]+)?$'
            AND (obs_value::numeric(10,3) BETWEEN 0 AND 100)
        ON CONFLICT (iso_code, sex, age, year) DO NOTHING;
        """,
    )


def fetch_population_api(settings: Settings) -> list[dict]:
    """
    Fetches population data from the configured API endpoint.

    Expected JSON object structure (based on live response preview):
    - `alpha3Code` -> iso_code (matches CSV `iso_code` values like "AFG")
    - `name` -> country
    - `population` -> integer population value
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(1, settings.api_fetch_retries + 1):
        try:
            req = urllib.request.Request(
                settings.api_url,
                headers={"User-Agent": "Mozilla/5.0 (etl_pipeline; +python)"},
            )
            with urllib.request.urlopen(req, timeout=settings.api_timeout_seconds) as resp:
                raw = resp.read()
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, list):
                raise RuntimeError(f"Unexpected API payload type: {type(payload)}")
            return payload
        except Exception as exc:  # noqa: BLE001 - demo script
            last_exc = exc
            print(
                f"[WARN] Population API fetch failed (attempt {attempt}/{settings.api_fetch_retries}): {exc!r}",
                file=sys.stderr,
            )
            time.sleep(1.0)
    raise RuntimeError(f"Could not fetch population API after retries: {last_exc!r}")


def seed_population_ingestion(conn: PGConnection, settings: Settings) -> None:
    """
    Ingestion layer implementation (new third source):
    - Calls `API_URL` and loads the response into `ingestion.population_raw`.
    - Stores only normalized fields:
        * iso_code, country, population
    """
    payload = fetch_population_api(settings)

    rows: list[tuple[str, str, int]] = []
    for item in payload:
        iso_code = item.get("alpha3Code")
        country = item.get("name")
        population = item.get("population")

        if not iso_code or not country or population is None:
            continue

        try:
            population_int = int(population)
        except Exception:
            continue

        if population_int <= 0:
            continue

        rows.append((str(iso_code), str(country), population_int))

    if not rows:
        raise RuntimeError("Population API returned no valid rows to load.")

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {settings.ingestion_schema}.population_raw (iso_code, country, population)
            VALUES (%s, %s, %s)
            ON CONFLICT (iso_code) DO UPDATE
            SET country = EXCLUDED.country,
                population = EXCLUDED.population,
                api_loaded_at = NOW();
            """,
            rows,
        )


def seed_population_staging(conn: PGConnection, settings: Settings) -> None:
    """
    Staging layer implementation (population):
    - Filters/validates `iso_code` shape and population positivity
    - Casts into a typed model stored in `staging.population_stg`
    """
    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.staging_schema}.population_stg (iso_code, country, population)
        SELECT
            iso_code,
            country,
            population::bigint
        FROM {settings.ingestion_schema}.population_raw
        WHERE
            iso_code ~ '^[A-Z]{{3}}$'
            AND population::bigint > 0
        ON CONFLICT (iso_code) DO UPDATE
        SET
            country = EXCLUDED.country,
            population = EXCLUDED.population,
            etl_loaded_at = NOW();
        """,
    )


def seed_population_mart(conn: PGConnection, settings: Settings) -> None:
    """
    Mart layer implementation (population + rate-to-count comparison):

    1) `mart.country_population`
       - A snapshot/dimension-like table mapping iso_code -> population.

    2) `mart.country_year_total_15plus_with_population`
       - Joins the rate slice (`mart.country_year_total_15plus`) with population.
       - Derives absolute estimated counts:
           employment_persons_est  = population * employment_rate / 100
           unemployment_persons_est = population * unemployment_rate / 100
       - Enables ranking by the number of unemployed persons (population-aware).
    """
    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.mart_schema}.country_population (iso_code, country, population)
        SELECT iso_code, country, population
        FROM {settings.staging_schema}.population_stg
        ON CONFLICT (iso_code) DO UPDATE
        SET
            country = EXCLUDED.country,
            population = EXCLUDED.population,
            snapshot_loaded_at = NOW();
        """,
    )

    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.mart_schema}.country_year_total_15plus_with_population (
            iso_code, country, year, population,
            employment_rate_15plus_total, unemployment_rate_15plus_total,
            unemployment_to_employment_ratio, unemployment_minus_employment,
            employment_persons_est, unemployment_persons_est
        )
        SELECT
            t.iso_code,
            t.country,
            t.year,
            p.population,
            t.employment_rate_15plus_total,
            t.unemployment_rate_15plus_total,
            t.unemployment_to_employment_ratio,
            t.unemployment_minus_employment,
            (p.population::numeric * t.employment_rate_15plus_total / 100)::numeric(22,2) AS employment_persons_est,
            (p.population::numeric * t.unemployment_rate_15plus_total / 100)::numeric(22,2) AS unemployment_persons_est
        FROM {settings.mart_schema}.country_year_total_15plus t
        JOIN {settings.mart_schema}.country_population p
          ON p.iso_code = t.iso_code
        ON CONFLICT (iso_code, year) DO UPDATE
        SET
            population = EXCLUDED.population,
            employment_rate_15plus_total = EXCLUDED.employment_rate_15plus_total,
            unemployment_rate_15plus_total = EXCLUDED.unemployment_rate_15plus_total,
            unemployment_to_employment_ratio = EXCLUDED.unemployment_to_employment_ratio,
            unemployment_minus_employment = EXCLUDED.unemployment_minus_employment,
            employment_persons_est = EXCLUDED.employment_persons_est,
            unemployment_persons_est = EXCLUDED.unemployment_persons_est,
            etl_loaded_at = NOW();
        """,
    )


def audit_source_snapshot(
    conn: PGConnection,
    settings: Settings,
    source_name: str,
    observed_rows: int,
    observed_fingerprint: int,
) -> None:
    """
    Persists per-source audit metrics and logs whether data changed.

    Change detection logic:
    - first_load: no previous snapshot for this source
    - unchanged: rows and fingerprint unchanged
    - row_count_changed: row count differs
    - content_changed_same_count: row count same but fingerprint differs
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT observed_rows, observed_fingerprint
            FROM {settings.mart_schema}.etl_source_audit
            WHERE source_name = %s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (source_name,),
        )
        prev = cur.fetchone()

    if prev is None:
        previous_rows = None
        previous_fingerprint = None
        delta_rows = None
        new_rows = observed_rows
        removed_rows = 0
        change_type = "first_load"
    else:
        previous_rows = int(prev[0])
        previous_fingerprint = int(prev[1])
        delta_rows = observed_rows - previous_rows
        new_rows = delta_rows if delta_rows > 0 else 0
        removed_rows = -delta_rows if delta_rows < 0 else 0
        if delta_rows != 0:
            change_type = "row_count_changed"
        elif observed_fingerprint != previous_fingerprint:
            change_type = "content_changed_same_count"
        else:
            change_type = "unchanged"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {settings.mart_schema}.etl_source_audit
                (source_name, observed_rows, observed_fingerprint,
                 previous_rows, previous_fingerprint,
                 delta_rows, new_rows, removed_rows, change_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                source_name,
                observed_rows,
                observed_fingerprint,
                previous_rows,
                previous_fingerprint,
                delta_rows,
                new_rows,
                removed_rows,
                change_type,
            ),
        )

    print(
        "[AUDIT] "
        f"source={source_name} "
        f"rows={observed_rows} "
        f"prev_rows={previous_rows if previous_rows is not None else 'n/a'} "
        f"delta_rows={delta_rows if delta_rows is not None else 'n/a'} "
        f"new_rows={new_rows} "
        f"removed_rows={removed_rows} "
        f"status={change_type}"
    )


def run_source_audit(conn: PGConnection, settings: Settings) -> None:
    """
    Computes source snapshots from ingestion tables and writes audit rows.
    """
    occup_rows, occup_fp = fetch_two_values(
        conn,
        f"""
        SELECT
            COUNT(*)::bigint AS cnt,
            COALESCE(
                bit_xor(
                    hashtextextended(
                        concat_ws('|', iso_code, country, sex, age, year, obs_value),
                        0
                    )
                ),
                0
            )::bigint AS fp
        FROM {settings.ingestion_schema}.occupazione_raw
        """,
    )
    audit_source_snapshot(conn, settings, "occupazione_csv", occup_rows, occup_fp)

    disoc_rows, disoc_fp = fetch_two_values(
        conn,
        f"""
        SELECT
            COUNT(*)::bigint AS cnt,
            COALESCE(
                bit_xor(
                    hashtextextended(
                        concat_ws('|', iso_code, country, sex, age, year, obs_value),
                        0
                    )
                ),
                0
            )::bigint AS fp
        FROM {settings.ingestion_schema}.disoccupazione_raw
        """,
    )
    audit_source_snapshot(conn, settings, "disoccupazione_csv", disoc_rows, disoc_fp)

    pop_rows, pop_fp = fetch_two_values(
        conn,
        f"""
        SELECT
            COUNT(*)::bigint AS cnt,
            COALESCE(
                bit_xor(
                    hashtextextended(
                        concat_ws('|', iso_code, country, population::text),
                        0
                    )
                ),
                0
            )::bigint AS fp
        FROM {settings.ingestion_schema}.population_raw
        """,
    )
    audit_source_snapshot(conn, settings, "population_api", pop_rows, pop_fp)


def seed_mart(conn: PGConnection, settings: Settings) -> None:
    """
    Mart layer implementation:
    - Joins `staging.occupazione_stg` and `staging.disoccupazione_stg` using the shared natural key
      (iso_code, sex, age, year) to produce an analytics-ready fact table.
    - Computes derived metrics:
        - unemployment_to_employment_ratio
        - unemployment_minus_employment
    - Builds a small aggregated slice for convenience/reporting:
        - Total unemployment/employment for age group 15+.
    - Resilience/idempotency:
        - `ON CONFLICT DO UPDATE` ensures reruns or duplicate join outputs never fail the pipeline.
    """
    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.mart_schema}.country_year_sex_age_rates
            (iso_code, country, sex, age, year,
             employment_rate, unemployment_rate,
             unemployment_to_employment_ratio, unemployment_minus_employment)
        SELECT
            e.iso_code,
            e.country,
            e.sex,
            e.age,
            e.year,
            e.employment_rate,
            u.unemployment_rate,
            (u.unemployment_rate / NULLIF(e.employment_rate, 0))::numeric(12,6) AS unemployment_to_employment_ratio,
            (u.unemployment_rate - e.employment_rate)::numeric(10,3) AS unemployment_minus_employment
        FROM {settings.staging_schema}.occupazione_stg e
        JOIN {settings.staging_schema}.disoccupazione_stg u
          ON u.iso_code = e.iso_code
         AND u.sex = e.sex
         AND u.age = e.age
         AND u.year = e.year
        ON CONFLICT (iso_code, sex, age, year) DO UPDATE
        SET
          employment_rate = EXCLUDED.employment_rate,
          unemployment_rate = EXCLUDED.unemployment_rate,
          unemployment_to_employment_ratio = EXCLUDED.unemployment_to_employment_ratio,
          unemployment_minus_employment = EXCLUDED.unemployment_minus_employment,
          etl_loaded_at = NOW();
        """,
    )

    exec_sql(
        conn,
        f"""
        INSERT INTO {settings.mart_schema}.country_year_total_15plus
            (iso_code, country, year,
             employment_rate_15plus_total, unemployment_rate_15plus_total,
             unemployment_to_employment_ratio, unemployment_minus_employment)
        SELECT
            iso_code,
            country,
            year,
            employment_rate::numeric(10,3) AS employment_rate_15plus_total,
            unemployment_rate::numeric(10,3) AS unemployment_rate_15plus_total,
            unemployment_to_employment_ratio::numeric(12,6),
            unemployment_minus_employment::numeric(10,3)
        FROM {settings.mart_schema}.country_year_sex_age_rates
        WHERE sex = 'Total' AND age = '15+'
        ON CONFLICT (iso_code, year) DO UPDATE
        SET
          employment_rate_15plus_total = EXCLUDED.employment_rate_15plus_total,
          unemployment_rate_15plus_total = EXCLUDED.unemployment_rate_15plus_total,
          unemployment_to_employment_ratio = EXCLUDED.unemployment_to_employment_ratio,
          unemployment_minus_employment = EXCLUDED.unemployment_minus_employment,
          etl_loaded_at = NOW();
        """,
    )


def create_indexes(conn: PGConnection, settings: Settings) -> None:
    exec_sql(
        conn,
        f"""
        CREATE INDEX IF NOT EXISTS idx_occupazione_stg_keys
            ON {settings.staging_schema}.occupazione_stg (iso_code, sex, age, year);
        CREATE INDEX IF NOT EXISTS idx_disoccupazione_stg_keys
            ON {settings.staging_schema}.disoccupazione_stg (iso_code, sex, age, year);

        -- Population staging indexes (API -> typed model)
        CREATE INDEX IF NOT EXISTS idx_population_stg_iso
            ON {settings.staging_schema}.population_stg (iso_code);
        CREATE INDEX IF NOT EXISTS idx_population_stg_population
            ON {settings.staging_schema}.population_stg (population);

        CREATE INDEX IF NOT EXISTS idx_mart_country_year_sex_age
            ON {settings.mart_schema}.country_year_sex_age_rates (country, year, sex, age);
        CREATE INDEX IF NOT EXISTS idx_mart_total_15plus
            ON {settings.mart_schema}.country_year_total_15plus (country, year);

        CREATE INDEX IF NOT EXISTS idx_mart_country_population_iso
            ON {settings.mart_schema}.country_population (iso_code);
        CREATE INDEX IF NOT EXISTS idx_mart_total_15plus_with_population_year
            ON {settings.mart_schema}.country_year_total_15plus_with_population (year, population);
        CREATE INDEX IF NOT EXISTS idx_mart_total_15plus_with_population_iso_year
            ON {settings.mart_schema}.country_year_total_15plus_with_population (iso_code, year);
        """,
    )


def main() -> None:
    settings = load_settings()

    print("[INFO] Connecting to Postgres...")
    conn = connect_with_retry(settings)
    try:
        conn.autocommit = False
        ensure_schemas(conn, [settings.ingestion_schema, settings.staging_schema, settings.mart_schema])
        ensure_tables(conn, settings)

        print("[INFO] Truncating existing layer tables...")
        truncate_tables(conn, settings)

        print("[INFO] Seeding ingestion layer from CSVs (raw text landing)...")
        seed_ingestion(conn, settings)

        occup_ingested = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.ingestion_schema}.occupazione_raw;"
        )
        disoc_ingested = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.ingestion_schema}.disoccupazione_raw;"
        )
        print(f"[INFO] Ingestion rows: occupazione={occup_ingested}, disoccupazione={disoc_ingested}")

        print("[INFO] Building staging layer (validate/cast/standardize)...")
        seed_staging(conn, settings)
        occup_staged = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.staging_schema}.occupazione_stg;"
        )
        disoc_staged = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.staging_schema}.disoccupazione_stg;"
        )
        print(f"[INFO] Staging rows: occupazione={occup_staged}, disoccupazione={disoc_staged}")

        print("[INFO] Building mart layer (join + analytics slices)...")
        seed_mart(conn, settings)
        mart_rows = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.mart_schema}.country_year_sex_age_rates;"
        )
        mart_total_rows = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.mart_schema}.country_year_total_15plus;"
        )
        print(f"[INFO] Mart rows: country_year_sex_age_rates={mart_rows}, total_15plus={mart_total_rows}")

        print("[INFO] Building population ingestion layer from API...")
        seed_population_ingestion(conn, settings)
        pop_ingested = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.ingestion_schema}.population_raw;"
        )
        print(f"[INFO] Population rows (ingestion): {pop_ingested}")

        print("[INFO] Auditing source changes (new/removed/content-changed rows)...")
        run_source_audit(conn, settings)

        print("[INFO] Building population staging layer (validate/cast)...")
        seed_population_staging(conn, settings)
        pop_staged = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.staging_schema}.population_stg;"
        )
        print(f"[INFO] Population rows (staging): {pop_staged}")

        print("[INFO] Building population mart join (rates -> counts)...")
        seed_population_mart(conn, settings)
        pop_mart_rows = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.mart_schema}.country_year_total_15plus_with_population;"
        )
        country_pop_rows = fetch_one_value(
            conn, f"SELECT COUNT(*) FROM {settings.mart_schema}.country_population;"
        )
        print(
            f"[INFO] Population mart rows: country_population={country_pop_rows}, "
            f"total_15plus_with_population={pop_mart_rows}"
        )

        print("[INFO] Creating indexes (performance)...")
        create_indexes(conn, settings)

        conn.commit()
        print("[SUCCESS] ETL pipeline completed successfully.")

    except Exception as exc:
        conn.rollback()
        print(f"[ERROR] ETL pipeline failed: {exc!r}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

