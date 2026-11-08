-- Série mensal preparada pela visão criada em sql/schema.sql.
SELECT
    reference_month,
    ipca_mom_pct,
    selic_target_month_end_annual_pct
FROM v_macro_monthly
ORDER BY reference_month;

-- Médias por ano para uma comparação exploratória.
SELECT
    substr(reference_month, 1, 4) AS reference_year,
    ROUND(AVG(ipca_mom_pct), 3) AS mean_monthly_ipca_pct,
    ROUND(AVG(selic_target_month_end_annual_pct), 3) AS mean_month_end_selic_pct
FROM v_macro_monthly
GROUP BY substr(reference_month, 1, 4)
ORDER BY reference_year;

-- Dez meses com maior variação mensal do IPCA.
SELECT reference_month, ipca_mom_pct
FROM v_macro_monthly
WHERE ipca_mom_pct IS NOT NULL
ORDER BY ipca_mom_pct DESC
LIMIT 10;
