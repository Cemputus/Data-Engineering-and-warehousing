import os
import sys
import time
from dataclasses import dataclass
from typing import Optional, Sequence

import psycopg2  # type: ignore[reportMissingImports]
from psycopg2.extensions import connection as PGConnection  # type: ignore[reportMissingImports]


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    occupazione_csv: str
    disoccupazione_csv: str

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
        occupazione_csv=os.getenv("OCCUPAZIONE_CSV", "/app/occupazione.csv"),
        disoccupazione_csv=os.getenv("DISOCCUPAZIONE_CSV", "/app/disoccupazione.csv"),
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

        CREATE INDEX IF NOT EXISTS idx_mart_country_year_sex_age
            ON {settings.mart_schema}.country_year_sex_age_rates (country, year, sex, age);
        CREATE INDEX IF NOT EXISTS idx_mart_total_15plus
            ON {settings.mart_schema}.country_year_total_15plus (country, year);
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

