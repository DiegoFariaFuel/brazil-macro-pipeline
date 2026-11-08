PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_series (
    series_code TEXT PRIMARY KEY,
    series_name TEXT NOT NULL,
    frequency TEXT NOT NULL CHECK (frequency IN ('daily', 'monthly')),
    unit TEXT NOT NULL,
    source_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_macro_observations (
    series_code TEXT NOT NULL,
    reference_date TEXT NOT NULL,
    value REAL NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (series_code, reference_date),
    FOREIGN KEY (series_code) REFERENCES dim_series(series_code),
    CHECK (date(reference_date) IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_macro_observations_date
    ON fact_macro_observations(reference_date);

DROP VIEW IF EXISTS v_macro_monthly;

CREATE VIEW v_macro_monthly AS
WITH ranked_monthly_observations AS (
    SELECT
        strftime('%Y-%m', reference_date) AS reference_month,
        series_code,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY series_code, strftime('%Y-%m', reference_date)
            ORDER BY reference_date DESC
        ) AS row_in_month
    FROM fact_macro_observations
),
month_values AS (
    SELECT
        reference_month,
        MAX(CASE
            WHEN series_code = '433' AND row_in_month = 1 THEN value
        END) AS ipca_mom_pct,
        MAX(CASE
            WHEN series_code = '432' AND row_in_month = 1 THEN value
        END) AS selic_target_month_end_annual_pct
    FROM ranked_monthly_observations
    GROUP BY reference_month
)
SELECT
    reference_month,
    ipca_mom_pct,
    selic_target_month_end_annual_pct
FROM month_values
ORDER BY reference_month;
