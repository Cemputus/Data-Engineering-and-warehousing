-- Demo queries for the 3-layer ETL Mart
-- Mart schemas/tables created by `etl_pipeline.py`:
--   mart.country_year_sex_age_rates
--   mart.country_year_total_15plus
--
-- Connection tip: connect to the same Postgres DB used by Docker (DB_NAME=de_wh).

-- 1) Basic sanity checks (row counts)
SELECT
  (SELECT COUNT(*) FROM mart.country_year_sex_age_rates) AS country_year_sex_age_rates_rows,
  (SELECT COUNT(*) FROM mart.country_year_total_15plus)  AS country_year_total_15plus_rows;

-- 2) How many records per year in the main mart fact table
SELECT
  year,
  COUNT(*) AS rows_in_fact
FROM mart.country_year_sex_age_rates
GROUP BY year
ORDER BY year;

-- 3) Latest-year ranking: Top 10 countries by unemployment/employment ratio
--    (using the Total, 15+ aggregated slice)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_total_15plus
)
SELECT
  t.iso_code,
  t.country,
  t.year,
  t.employment_rate_15plus_total,
  t.unemployment_rate_15plus_total,
  t.unemployment_to_employment_ratio,
  t.unemployment_minus_employment
FROM mart.country_year_total_15plus t
JOIN latest_year ly ON ly.y = t.year
ORDER BY t.unemployment_to_employment_ratio DESC
LIMIT 10;

-- 4) Time-series slice for one country (example: Afghanistan - AFG)
SELECT
  year,
  employment_rate_15plus_total,
  unemployment_rate_15plus_total,
  unemployment_to_employment_ratio,
  unemployment_minus_employment
FROM mart.country_year_total_15plus
WHERE iso_code = 'AFG'
ORDER BY year;

-- 5) Sex/age breakdown for a specific year (example: latest year)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_sex_age_rates
)
SELECT
  r.year,
  r.sex,
  r.age,
  AVG(r.employment_rate)   AS avg_employment_rate,
  AVG(r.unemployment_rate) AS avg_unemployment_rate
FROM mart.country_year_sex_age_rates r
JOIN latest_year ly ON ly.y = r.year
GROUP BY r.year, r.sex, r.age
ORDER BY r.sex, r.age;

-- 6) Example of a derived-metric “gap” (unemployment - employment)
--    Latest year, Top 10 countries by the gap.
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_sex_age_rates
)
SELECT
  r.iso_code,
  r.country,
  r.year,
  r.sex,
  r.age,
  r.unemployment_minus_employment
FROM mart.country_year_sex_age_rates r
JOIN latest_year ly ON ly.y = r.year
ORDER BY r.unemployment_minus_employment DESC
LIMIT 10;

-- 7) Counts: how many distinct countries exist in the latest year (main mart fact)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_sex_age_rates
)
SELECT
  r.year,
  COUNT(DISTINCT r.iso_code) AS distinct_countries
FROM mart.country_year_sex_age_rates r
JOIN latest_year ly ON ly.y = r.year
GROUP BY r.year;

-- 8) Trend: average unemployment/employment rates across all countries per year (Total, 15+)
SELECT
  year,
  AVG(employment_rate_15plus_total)   AS avg_employment_rate_15plus_total,
  AVG(unemployment_rate_15plus_total) AS avg_unemployment_rate_15plus_total,
  AVG(unemployment_to_employment_ratio) AS avg_unemployment_to_employment_ratio
FROM mart.country_year_total_15plus
GROUP BY year
ORDER BY year;

-- 9) Latest-year Top 10 by unemployment rate (Total, 15+)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_total_15plus
)
SELECT
  t.iso_code,
  t.country,
  t.year,
  t.unemployment_rate_15plus_total,
  t.employment_rate_15plus_total
FROM mart.country_year_total_15plus t
JOIN latest_year ly ON ly.y = t.year
ORDER BY t.unemployment_rate_15plus_total DESC
LIMIT 10;

-- 10) Latest-year Top 10 by employment rate (lowest) (Total, 15+)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_total_15plus
)
SELECT
  t.iso_code,
  t.country,
  t.year,
  t.employment_rate_15plus_total,
  t.unemployment_rate_15plus_total
FROM mart.country_year_total_15plus t
JOIN latest_year ly ON ly.y = t.year
ORDER BY t.employment_rate_15plus_total ASC
LIMIT 10;

-- 11) Latest-year Top 10 by employment rate (highest) (Total, 15+)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_total_15plus
)
SELECT
  t.iso_code,
  t.country,
  t.year,
  t.employment_rate_15plus_total,
  t.unemployment_rate_15plus_total
FROM mart.country_year_total_15plus t
JOIN latest_year ly ON ly.y = t.year
ORDER BY t.employment_rate_15plus_total DESC
LIMIT 10;

-- 12) Trend: Afghanistan (AFG) unemployment rate and derived unemployment/employment ratio (Total, 15+)
SELECT
  year,
  unemployment_rate_15plus_total,
  unemployment_to_employment_ratio
FROM mart.country_year_total_15plus
WHERE iso_code = 'AFG'
ORDER BY year;

-- 13) Counts: rows per (sex, age) in the latest year (main mart fact)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_sex_age_rates
)
SELECT
  r.year,
  r.sex,
  r.age,
  COUNT(*) AS rows
FROM mart.country_year_sex_age_rates r
JOIN latest_year ly ON ly.y = r.year
GROUP BY r.year, r.sex, r.age
ORDER BY r.sex, r.age;

-- 14) Latest-year Top 10 by unemployment "gap" (unemployment - employment) (Total, 15+)
WITH latest_year AS (
  SELECT MAX(year) AS y
  FROM mart.country_year_total_15plus
)
SELECT
  t.iso_code,
  t.country,
  t.year,
  t.unemployment_minus_employment,
  t.unemployment_rate_15plus_total,
  t.employment_rate_15plus_total
FROM mart.country_year_total_15plus t
JOIN latest_year ly ON ly.y = t.year
ORDER BY t.unemployment_minus_employment DESC
LIMIT 10;

