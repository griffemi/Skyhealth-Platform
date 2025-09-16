from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET = os.environ.get("BQ_DATASET", "skyhealth_dev_gold")
TABLE_NAME = os.environ.get("BQ_GOLD_TABLE", "climate_daily_summary")
FULL_TABLE = f"`{PROJECT_ID}.{DATASET}.{TABLE_NAME}`" if PROJECT_ID else f"`{DATASET}.{TABLE_NAME}`"

client = bigquery.Client(project=PROJECT_ID) if PROJECT_ID else bigquery.Client()


@st.cache_data(ttl=300)
def load_available_dates(limit: int = 30) -> list[date]:
    query = f"""
        SELECT DISTINCT observation_date
        FROM {FULL_TABLE}
        ORDER BY observation_date DESC
        LIMIT {limit}
    """
    result = client.query(query).result()
    return [row.observation_date for row in result]


@st.cache_data(ttl=300)
def load_regions() -> list[str]:
    query = f"SELECT DISTINCT region_id FROM {FULL_TABLE} ORDER BY region_id"
    return [row.region_id for row in client.query(query).result()]


@st.cache_data(ttl=300)
def fetch_region_snapshot(observation_date: date) -> pd.DataFrame:
    query = f"""
        SELECT region_id, avg_temp_c, total_prcp_mm, had_precip, avg_hdd18, avg_cdd18
        FROM {FULL_TABLE}
        WHERE observation_date = @observation_date
        ORDER BY region_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("observation_date", "DATE", observation_date),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    df["observation_date"] = observation_date
    return df


@st.cache_data(ttl=300)
def fetch_region_history(region_id: str, days: int = 30) -> pd.DataFrame:
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    query = f"""
        SELECT observation_date, avg_temp_c, total_prcp_mm, avg_hdd18, avg_cdd18, had_precip
        FROM {FULL_TABLE}
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
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


@st.cache_data(ttl=300)
def fetch_precip_windows(region_id: str, window_days: tuple[int, int] = (7, 30)) -> pd.DataFrame:
    short_window, long_window = window_days
    query = f"""
        SELECT
          observation_date,
          region_id,
          AVG(avg_temp_c) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS avg_temp_{short_window}d,
          SUM(total_prcp_mm) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {short_window - 1} PRECEDING AND CURRENT ROW) AS prcp_{short_window}d,
          SUM(total_prcp_mm) OVER (PARTITION BY region_id ORDER BY observation_date ROWS BETWEEN {long_window - 1} PRECEDING AND CURRENT ROW) AS prcp_{long_window}d
        FROM {FULL_TABLE}
        WHERE region_id = @region_id
        ORDER BY observation_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("region_id", "STRING", region_id)]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


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

available_dates = load_available_dates()
if not available_dates:
    st.warning("No gold data available yet. Ingest and publish a partition to continue.")
    st.stop()

selected_date = st.sidebar.selectbox(
    "Observation Date",
    available_dates,
    format_func=lambda d: d.strftime("%Y-%m-%d"),
)
regions = load_regions()

page = st.sidebar.radio("View", list(PAGES.keys()))

if page == "Region Summary":
    PAGES[page](selected_date)
else:
    PAGES[page](regions)
