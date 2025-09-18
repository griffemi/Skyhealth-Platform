from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from google.api_core import exceptions as gexc
from google.auth import exceptions as auth_exc
from google.cloud import bigquery

from skyhealth.clickhouse_io import ClickHouseError, query_dataframe
from skyhealth.config import settings

IS_PROD = settings.env.lower() == "prod"
BQ_PROJECT = os.environ.get("GCP_PROJECT") or settings.bigquery_project or settings.project_id
BQ_DATASET = os.environ.get("BQ_DATASET", settings.bigquery_dataset)
BQ_TABLE_NAME = os.environ.get("BQ_GOLD_TABLE", "climate_daily_summary")
CH_TABLE = settings.clickhouse_table_identifier()


class DataSourceUnavailable(RuntimeError):
    """Raised when the configured analytics store cannot be reached."""


@st.cache_resource(show_spinner=False)
def get_bigquery_client() -> bigquery.Client:
    if not BQ_PROJECT:
        raise DataSourceUnavailable(
            "BigQuery project is undefined. Set `GCP_PROJECT`/`BIGQUERY_PROJECT` in prod before launching the dashboard."
        )
    try:
        return bigquery.Client(project=BQ_PROJECT)
    except (auth_exc.DefaultCredentialsError, auth_exc.RefreshError) as exc:  # pragma: no cover - requires env state
        raise DataSourceUnavailable(
            "No Google Cloud credentials available. Run `gcloud auth application-default login` "
            "or set `GOOGLE_APPLICATION_CREDENTIALS` before launching the dashboard."
        ) from exc


def ensure_bigquery_table(client: bigquery.Client) -> str:
    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
    table_ref = bigquery.TableReference(dataset_ref, BQ_TABLE_NAME)
    try:
        client.get_table(table_ref)
    except gexc.NotFound as exc:
        raise DataSourceUnavailable(
            f"BigQuery table `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_NAME}` is missing. "
            "Run `make publish-bq` in prod or update `BQ_DATASET`/`BQ_GOLD_TABLE`."
        ) from exc
    except gexc.Forbidden as exc:
        raise DataSourceUnavailable(
            "Current credentials lack BigQuery permissions. Confirm your account has access to "
            f"`{BQ_PROJECT}.{BQ_DATASET}` or switch projects via `gcloud config set project`."
        ) from exc
    return f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_NAME}`"


def run_clickhouse_query(sql: str, parameters: dict[str, object] | None = None) -> pd.DataFrame:
    try:
        return query_dataframe(sql, parameters=parameters)
    except ClickHouseError as exc:
        raise DataSourceUnavailable(
            "Unable to query ClickHouse. Ensure the local ClickHouse server is running (for example: "
            "`docker run -p 8123:8123 clickhouse/clickhouse-server`) and the `clickhouse_*` settings are correct."
        ) from exc


@st.cache_data(ttl=300)
def load_available_dates(limit: int = 30) -> list[date]:
    if IS_PROD:
        client = get_bigquery_client()
        table = ensure_bigquery_table(client)
        query = f"""
            SELECT DISTINCT observation_date
            FROM {table}
            ORDER BY observation_date DESC
            LIMIT {int(limit)}
        """
        result = client.query(query).result()
        return [row.observation_date for row in result]

    sql = f"""
        SELECT DISTINCT observation_date
        FROM {CH_TABLE}
        ORDER BY observation_date DESC
        LIMIT {int(limit)}
    """
    df = run_clickhouse_query(sql)
    return [pd.to_datetime(value).date() for value in df["observation_date"].tolist()]


@st.cache_data(ttl=300)
def load_regions() -> list[str]:
    if IS_PROD:
        client = get_bigquery_client()
        table = ensure_bigquery_table(client)
        query = f"SELECT DISTINCT region_id FROM {table} ORDER BY region_id"
        return [row.region_id for row in client.query(query).result()]

    sql = f"SELECT DISTINCT region_id FROM {CH_TABLE} ORDER BY region_id"
    df = run_clickhouse_query(sql)
    return df["region_id"].astype(str).tolist()


@st.cache_data(ttl=300)
def fetch_region_snapshot(observation_date: date) -> pd.DataFrame:
    if IS_PROD:
        client = get_bigquery_client()
        table = ensure_bigquery_table(client)
        query = f"""
            SELECT region_id, avg_temp_c, total_prcp_mm, had_precip, avg_hdd18, avg_cdd18
            FROM {table}
            WHERE observation_date = @observation_date
            ORDER BY region_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("observation_date", "DATE", observation_date)]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        df["observation_date"] = observation_date
        return df

    sql = f"""
        SELECT region_id, avg_temp_c, total_prcp_mm, had_precip, avg_hdd18, avg_cdd18
        FROM {CH_TABLE}
        WHERE observation_date = %(observation_date)s
        ORDER BY region_id
    """
    df = run_clickhouse_query(sql, {"observation_date": observation_date})
    df["observation_date"] = observation_date
    return df


@st.cache_data(ttl=300)
def fetch_region_history(region_id: str, days: int = 30) -> pd.DataFrame:
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    if IS_PROD:
        client = get_bigquery_client()
        table = ensure_bigquery_table(client)
        query = f"""
            SELECT observation_date, avg_temp_c, total_prcp_mm, avg_hdd18, avg_cdd18, had_precip
            FROM {table}
            WHERE region_id = @region_id AND observation_date BETWEEN @start_date AND @end_date
            ORDER BY observation_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("region_id", "STRING", region_id),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        return client.query(query, job_config=job_config).to_dataframe()

    sql = f"""
        SELECT observation_date, avg_temp_c, total_prcp_mm, avg_hdd18, avg_cdd18, had_precip
        FROM {CH_TABLE}
        WHERE region_id = %(region_id)s AND observation_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY observation_date
    """
    return run_clickhouse_query(
        sql,
        {
            "region_id": region_id,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


@st.cache_data(ttl=300)
def fetch_precip_windows(region_id: str, window_days: tuple[int, int] = (7, 30)) -> pd.DataFrame:
    short_window, long_window = window_days

    if IS_PROD:
        client = get_bigquery_client()
        table = ensure_bigquery_table(client)
        query = f"""
            SELECT
              observation_date,
              region_id,
              AVG(avg_temp_c) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS avg_temp_{short_window}d,
              SUM(total_prcp_mm) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS prcp_{short_window}d,
              SUM(total_prcp_mm) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS prcp_{long_window}d
            FROM {table}
            WHERE region_id = @region_id
            ORDER BY observation_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("region_id", "STRING", region_id)]
        )
        return client.query(query, job_config=job_config).to_dataframe()

    sql = f"""
        SELECT
          observation_date,
          region_id,
          avg(avg_temp_c) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS avg_temp_{short_window}d,
          sum(total_prcp_mm) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS prcp_{short_window}d,
          sum(total_prcp_mm) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS prcp_{long_window}d
        FROM {CH_TABLE}
        WHERE region_id = %(region_id)s
        ORDER BY observation_date
    """
    return run_clickhouse_query(sql, {"region_id": region_id})


def region_summary_page(selected_date: date) -> None:
    st.header("Region Summary")
    snapshot = fetch_region_snapshot(selected_date)
    if snapshot.empty:
        st.info("No data available for the selected date yet.")
        return
    st.metric("Regions", snapshot["region_id"].nunique())
    st.metric("Total precipitation (mm)", f"{snapshot['total_prcp_mm'].sum():.1f}")
    st.metric("Average temperature (°C)", f"{snapshot['avg_temp_c'].mean():.1f}")
    st.dataframe(snapshot, use_container_width=True)


def station_details_page(regions: list[str]) -> None:
    st.header("Station Details")
    region = st.selectbox("Choose a region", regions)
    history = fetch_region_history(region)
    if history.empty:
        st.info("No history available for this region.")
        return
    history["observation_date"] = pd.to_datetime(history["observation_date"])
    st.line_chart(history.set_index("observation_date")["avg_temp_c"], height=220)
    st.bar_chart(history.set_index("observation_date")["total_prcp_mm"], height=220)
    st.dataframe(history.sort_values("observation_date", ascending=False), use_container_width=True)


def precipitation_windows_page(regions: list[str]) -> None:
    st.header("Precipitation Windows")
    region = st.selectbox("Region", regions, key="precip-region")
    windows = fetch_precip_windows(region)
    if windows.empty:
        st.info("No precipitation trends available.")
        return
    windows["observation_date"] = pd.to_datetime(windows["observation_date"])
    chart_df = windows.set_index("observation_date")[["prcp_7d", "prcp_30d"]]
    st.area_chart(chart_df, height=260)
    st.dataframe(windows.sort_values("observation_date", ascending=False), use_container_width=True)


PAGES = {
    "Region Summary": region_summary_page,
    "Station Details": station_details_page,
    "Precipitation Windows": precipitation_windows_page,
}

st.set_page_config(page_title="Skyhealth Climate Dashboard", layout="wide")
st.title("Skyhealth Climate Dashboard")

try:
    available_dates = load_available_dates()
except DataSourceUnavailable as exc:
    st.error(str(exc))
    st.stop()
except gexc.GoogleAPICallError as exc:  # pragma: no cover - network failure
    st.error(f"BigQuery query failed: {exc.message}")
    st.stop()

if not available_dates:
    st.warning("No gold data available yet. Ingest and publish a partition to continue.")
    st.stop()

selected_date = st.sidebar.selectbox(
    "Observation Date",
    available_dates,
    format_func=lambda d: d.strftime("%Y-%m-%d"),
)

try:
    regions = load_regions()
except DataSourceUnavailable as exc:
    st.error(str(exc))
    st.stop()
except gexc.GoogleAPICallError as exc:  # pragma: no cover - network failure
    st.error(f"BigQuery query failed: {exc.message}")
    st.stop()

page = st.sidebar.radio("View", list(PAGES.keys()))

if page == "Region Summary":
    PAGES[page](selected_date)
else:
    PAGES[page](regions)
