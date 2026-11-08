"""Ingest BCB SGS series, validate them, model them in SQLite, and report."""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
from plotly.subplots import make_subplots


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERIES_CONFIG = PROJECT_ROOT / "config" / "series.json"
SCHEMA_SQL = PROJECT_ROOT / "sql" / "schema.sql"
DEFAULT_DB = PROJECT_ROOT / "data" / "processed" / "macro.sqlite"
DEFAULT_OUTPUTS = PROJECT_ROOT / "outputs"
API_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
REQUEST_TIMEOUT_SECONDS = 30
MAX_QUERY_YEARS = 9

LOGGER = logging.getLogger("brazil_macro_pipeline")


class PipelineError(RuntimeError):
    """Raised when extraction or data-quality requirements are not met."""


def load_series_config(path: Path = SERIES_CONFIG) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8") as config_file:
        config = json.load(config_file)
    if not isinstance(config, dict) or not config:
        raise PipelineError("A configuração de séries está vazia ou inválida.")
    return config


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        # Handle February 29 when adding years from a leap-day start date.
        return value.replace(year=value.year + years, day=28)


def date_chunks(start_date: date, end_date: date) -> Iterator[tuple[date, date]]:
    """Yield inclusive ranges shorter than the BCB daily-series limit."""
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(_add_years(cursor, MAX_QUERY_YEARS) - timedelta(days=1), end_date)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def _parse_bcb_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"Data inválida recebida do SGS: {value!r}") from exc


def _parse_bcb_value(value: Any) -> float:
    if value is None:
        raise PipelineError("O SGS retornou uma observação sem valor.")
    normalized = str(value).strip().replace(",", ".")
    try:
        parsed = float(normalized)
    except ValueError as exc:
        raise PipelineError(f"Valor numérico inválido recebido do SGS: {value!r}") from exc
    if not math.isfinite(parsed):
        raise PipelineError(f"Valor não finito recebido do SGS: {value!r}")
    return parsed


def fetch_series(
    session: requests.Session,
    series_code: str,
    metadata: dict[str, str],
    start_date: date,
    end_date: date,
    max_attempts: int = 3,
) -> pd.DataFrame:
    """Fetch a BCB time series in bounded date windows."""
    records: list[dict[str, Any]] = []
    endpoint = API_BASE_URL.format(series_code=series_code)

    for chunk_start, chunk_end in date_chunks(start_date, end_date):
        params = {
            "formato": "json",
            "dataInicial": chunk_start.strftime("%d/%m/%Y"),
            "dataFinal": chunk_end.strftime("%d/%m/%Y"),
        }
        response: requests.Response | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = session.get(endpoint, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == max_attempts:
                    raise PipelineError(
                        f"Falha ao consultar a série SGS {series_code} "
                        f"({params['dataInicial']} a {params['dataFinal']}): {exc}"
                    ) from exc
                pause_seconds = min(2**attempt, 8)
                LOGGER.warning("Tentativa %s falhou; nova tentativa em %ss", attempt, pause_seconds)
                time.sleep(pause_seconds)

        assert response is not None
        try:
            payload = response.json()
        except requests.JSONDecodeError as exc:
            raise PipelineError(f"A resposta da série SGS {series_code} não é JSON válido.") from exc
        if not isinstance(payload, list):
            raise PipelineError(f"Formato inesperado na resposta da série SGS {series_code}.")

        for item in payload:
            if not isinstance(item, dict) or "data" not in item or "valor" not in item:
                raise PipelineError(f"Registro inesperado recebido para a série SGS {series_code}.")
            records.append(
                {
                    "series_code": str(series_code),
                    "series_name": metadata["name"],
                    "frequency": metadata["frequency"],
                    "unit": metadata["unit"],
                    "source_url": metadata["source"],
                    "reference_date": _parse_bcb_date(item["data"]),
                    "value": _parse_bcb_value(item["valor"]),
                }
            )

    return pd.DataFrame.from_records(records)


def validate_observations(observations: pd.DataFrame) -> pd.DataFrame:
    """Apply structural and value checks before writing to the database."""
    required_columns = {
        "series_code",
        "series_name",
        "frequency",
        "unit",
        "source_url",
        "reference_date",
        "value",
    }
    missing_columns = required_columns.difference(observations.columns)
    if missing_columns:
        raise PipelineError(f"Colunas obrigatórias ausentes: {sorted(missing_columns)}")
    if observations.empty:
        raise PipelineError("A API não retornou observações no intervalo solicitado.")
    if observations["reference_date"].isna().any():
        raise PipelineError("Há datas ausentes na extração.")
    if observations["value"].isna().any():
        raise PipelineError("Há valores ausentes na extração.")
    if not np.isfinite(observations["value"].astype(float)).all():
        raise PipelineError("Há valores infinitos ou não finitos na extração.")
    duplicate_mask = observations.duplicated(["series_code", "reference_date"], keep=False)
    if duplicate_mask.any():
        examples = observations.loc[duplicate_mask, ["series_code", "reference_date"]].head(5)
        raise PipelineError(f"Observações duplicadas encontradas:\n{examples.to_string(index=False)}")

    validated = observations.copy()
    validated["reference_date"] = pd.to_datetime(validated["reference_date"])
    validated["value"] = validated["value"].astype(float)
    return validated.sort_values(["series_code", "reference_date"]).reset_index(drop=True)


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))
    connection.commit()


def upsert_observations(
    connection: sqlite3.Connection,
    observations: pd.DataFrame,
    config: dict[str, dict[str, str]],
) -> None:
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    series_rows = [
        (code, item["name"], item["frequency"], item["unit"], item["source"])
        for code, item in config.items()
    ]
    connection.executemany(
        """
        INSERT INTO dim_series (series_code, series_name, frequency, unit, source_url)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(series_code) DO UPDATE SET
            series_name = excluded.series_name,
            frequency = excluded.frequency,
            unit = excluded.unit,
            source_url = excluded.source_url
        """,
        series_rows,
    )

    observation_rows = [
        (
            row.series_code,
            row.reference_date.strftime("%Y-%m-%d"),
            float(row.value),
            fetched_at,
        )
        for row in observations.itertuples(index=False)
    ]
    connection.executemany(
        """
        INSERT INTO fact_macro_observations
            (series_code, reference_date, value, fetched_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(series_code, reference_date) DO UPDATE SET
            value = excluded.value,
            fetched_at = excluded.fetched_at
        """,
        observation_rows,
    )
    connection.commit()


def read_monthly_mart(connection: sqlite3.Connection) -> pd.DataFrame:
    monthly = pd.read_sql_query(
        """
        SELECT reference_month, ipca_mom_pct, selic_target_month_end_annual_pct
        FROM v_macro_monthly
        ORDER BY reference_month
        """,
        connection,
    )
    if monthly.empty:
        raise PipelineError("A visão mensal está vazia; confira o intervalo e a carga.")

    monthly["ipca_12m_compounded_pct"] = (
        (1 + monthly["ipca_mom_pct"].div(100))
        .rolling(window=12, min_periods=12)
        .apply(np.prod, raw=True)
        .sub(1)
        .mul(100)
    )
    return monthly


def write_dashboard(monthly: pd.DataFrame, output_path: Path) -> None:
    chart_data = monthly.copy()
    chart_data["reference_month"] = pd.to_datetime(chart_data["reference_month"] + "-01")

    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        subplot_titles=("Inflação: variação mensal e acumulada em 12 meses", "Meta Selic no fim de cada mês"),
    )
    figure.add_trace(
        go.Bar(
            x=chart_data["reference_month"],
            y=chart_data["ipca_mom_pct"],
            name="IPCA mensal (%)",
            marker_color="#167c80",
            opacity=0.72,
            hovertemplate="%{x|%b/%Y}<br>IPCA no mês: %{y:.2f}%<extra></extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=chart_data["reference_month"],
            y=chart_data["ipca_12m_compounded_pct"],
            name="IPCA 12 meses (%)",
            mode="lines",
            line={"color": "#18324b", "width": 2.5},
            hovertemplate="%{x|%b/%Y}<br>IPCA em 12 meses: %{y:.2f}%<extra></extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=chart_data["reference_month"],
            y=chart_data["selic_target_month_end_annual_pct"],
            name="Meta Selic (% a.a.)",
            mode="lines",
            line={"color": "#c35b33", "width": 2.5, "shape": "hv"},
            hovertemplate="%{x|%b/%Y}<br>Meta Selic: %{y:.2f}% a.a.<extra></extra>",
        ),
        row=2,
        col=1,
    )
    figure.update_layout(
        title={"text": "Brazil Macro Pulse", "x": 0.03, "xanchor": "left"},
        template="plotly_white",
        height=760,
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        margin={"l": 65, "r": 35, "t": 110, "b": 45},
        font={"family": "Arial, sans-serif", "color": "#24313d"},
    )
    figure.update_yaxes(title_text="Variação (%)", ticksuffix="%", row=1, col=1)
    figure.update_yaxes(title_text="Meta anual", ticksuffix="%", row=2, col=1)
    figure.update_xaxes(title_text="Mês", row=2, col=1, rangeslider_visible=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.write_html(output_path, include_plotlyjs="cdn", full_html=True)


def _format_percent(value: float) -> str:
    return f"{value:.2f}%".replace(".", ",")


def write_analysis_summary(monthly: pd.DataFrame, output_path: Path) -> None:
    ipca = monthly.dropna(subset=["ipca_mom_pct"])
    selic = monthly.dropna(subset=["selic_target_month_end_annual_pct"])
    if ipca.empty or selic.empty:
        raise PipelineError("Não há observações suficientes para gerar o resumo exploratório.")

    latest_ipca = ipca.iloc[-1]
    latest_selic = selic.iloc[-1]
    highest_ipca = ipca.loc[ipca["ipca_mom_pct"].idxmax()]
    missing_ipca_months = monthly.loc[monthly["ipca_mom_pct"].isna(), "reference_month"].tolist()
    missing_label = "mês" if len(missing_ipca_months) == 1 else "meses"
    latest_ipca_12m = latest_ipca["ipca_12m_compounded_pct"]
    ipca_12m_text = (
        _format_percent(float(latest_ipca_12m))
        if pd.notna(latest_ipca_12m)
        else "indisponível (menos de 12 meses de dados)"
    )

    lines = [
        "# Resumo exploratório — Brazil Macro Pulse",
        "",
        f"Recorte disponível na execução: **{ipca.iloc[0]['reference_month']} a {monthly.iloc[-1]['reference_month']}**.",
        "",
        "## Leituras do conjunto",
        "",
        f"- IPCA mensal mais recente: **{_format_percent(float(latest_ipca['ipca_mom_pct']))}** "
        f"({latest_ipca['reference_month']}).",
        f"- IPCA composto em 12 meses mais recente: **{ipca_12m_text}** "
        f"({latest_ipca['reference_month']}).",
        f"- Meta Selic no último mês disponível: **{_format_percent(float(latest_selic['selic_target_month_end_annual_pct']))} a.a.** "
        f"({latest_selic['reference_month']}; último valor diário do mês).",
        f"- Maior variação mensal do IPCA no recorte: **{_format_percent(float(highest_ipca['ipca_mom_pct']))}** "
        f"({highest_ipca['reference_month']}).",
        "",
        "## Qualidade e interpretação",
        "",
        f"A visão mensal contém {len(monthly)} meses. Há {len(missing_ipca_months)} {missing_label} sem IPCA publicado "
        f"na data da coleta: {', '.join(missing_ipca_months) if missing_ipca_months else 'nenhum'}.",
        "A Meta Selic diária foi resumida usando o último valor observado em cada mês; não representa uma média mensal.",
        "As séries mostram movimentos ao longo do tempo, mas este resumo não testa causalidade entre juros e inflação.",
        "",
        "Os valores são atualizados quando o pipeline é executado; consulte o CSV e o painel para o recorte completo.",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_pipeline(
    start_date: date,
    end_date: date,
    db_path: Path = DEFAULT_DB,
    output_dir: Path = DEFAULT_OUTPUTS,
) -> None:
    if start_date > end_date:
        raise PipelineError("A data inicial deve ser anterior ou igual à data final.")

    config = load_series_config()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "Brazil-Macro-Pulse/1.0 (public portfolio project)"})
    extracted: list[pd.DataFrame] = []
    for code, metadata in config.items():
        LOGGER.info("Extraindo SGS %s — %s", code, metadata["name"])
        series = fetch_series(session, code, metadata, start_date, end_date)
        LOGGER.info("Recebidas %s observações da série %s", len(series), code)
        extracted.append(series)

    observations = validate_observations(pd.concat(extracted, ignore_index=True))
    with sqlite3.connect(db_path) as connection:
        initialize_database(connection)
        upsert_observations(connection, observations, config)
        monthly = read_monthly_mart(connection)

    csv_path = output_dir / "macro_monthly.csv"
    dashboard_path = output_dir / "dashboard.html"
    summary_path = output_dir / "analysis_summary.md"
    monthly.to_csv(csv_path, index=False, float_format="%.6f")
    write_dashboard(monthly, dashboard_path)
    write_analysis_summary(monthly, summary_path)

    LOGGER.info(
        "Concluído: %s observações, %s meses; CSV: %s; painel: %s; resumo: %s; SQLite: %s",
        len(observations),
        len(monthly),
        csv_path,
        dashboard_path,
        summary_path,
        db_path,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extrai IPCA e Meta Selic do SGS/BCB, valida, grava em SQLite e cria um painel."
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2015, 1, 1),
        help="Primeira data, no formato AAAA-MM-DD (padrão: 2015-01-01).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Última data, no formato AAAA-MM-DD (padrão: hoje).",
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB, help="Arquivo SQLite de destino.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUTS, help="Pasta de CSV e painel.")
    parser.add_argument("--verbose", action="store_true", help="Exibe detalhes da execução.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        run_pipeline(args.start_date, args.end_date, args.db_path, args.output_dir)
    except (PipelineError, OSError, sqlite3.Error) as exc:
        LOGGER.error("Pipeline interrompido: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
