from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from clickhouse_connect import get_client
from clickhouse_connect.driver import exceptions as ch_exc

from pipelines.config import settings

__all__ = [
    "ClickHouseError",
    "ClickHousePublisher",
    "query_dataframe",
]

ClickHouseError = ch_exc.ClickHouseError

_TABLE_COLUMNS = [
    "region_id",
    "source",
    "observation_date",
    "ingest_date",
    "avg_temp_c",
    "total_prcp_mm",
    "had_precip",
    "avg_hdd18",
    "avg_cdd18",
]

_SCHEMA_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {table} (
    region_id String,
    source String,
    observation_date Date,
    ingest_date Date,
    avg_temp_c Float32,
    total_prcp_mm Float32,
    had_precip String,
    avg_hdd18 Float32,
    avg_cdd18 Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observation_date)
ORDER BY (region_id, observation_date)
"""

_schema_ready: bool = False


def _client_kwargs(database: str | None = None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "host": settings.clickhouse_host,
        "port": settings.clickhouse_port,
        "username": settings.clickhouse_user,
        "secure": settings.clickhouse_secure,
    }
    if settings.clickhouse_password:
        kwargs["password"] = settings.clickhouse_password
    if settings.clickhouse_secure:
        kwargs["verify"] = settings.clickhouse_verify
    kwargs["database"] = database or settings.clickhouse_database
    return kwargs


def ensure_schema(force: bool = False) -> None:
    """Ensure the ClickHouse database and table exist."""
    global _schema_ready
    if _schema_ready and not force:
        return
    client = get_client(**_client_kwargs())
    client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
    client.command(_SCHEMA_TEMPLATE.format(table=_fq_table()))
    client.close()
    _schema_ready = True


def _fq_table() -> str:
    return f"{settings.clickhouse_database}.{settings.clickhouse_table}"


def _prepare_dataframe(pdf: pd.DataFrame, target_date: date) -> pd.DataFrame:
    df = pdf.copy()
    df = df[_TABLE_COLUMNS]
    df["observation_date"] = pd.to_datetime(df["observation_date"]).dt.date
    df["ingest_date"] = pd.to_datetime(df["ingest_date"]).dt.date
    df["had_precip"] = df["had_precip"].astype(str)
    return df


def query_dataframe(sql: str, parameters: dict[str, Any] | None = None) -> pd.DataFrame:
    ensure_schema()
    client = get_client(**_client_kwargs())
    try:
        return client.query_df(sql, parameters=parameters)
    finally:
        client.close()


class ClickHousePublisher:
    """Lightweight wrapper to publish Gold partitions into ClickHouse."""

    def publish(self, pdf: pd.DataFrame, target_date: date) -> None:
        ensure_schema()
        df = _prepare_dataframe(pdf, target_date)
        client = get_client(**_client_kwargs())
        try:
            client.command(
                f"ALTER TABLE {_fq_table()} DELETE WHERE observation_date = toDate('%s') SETTINGS mutations_sync = 1"
                % target_date.isoformat()
            )
            client.insert_df(settings.clickhouse_table, df, column_names=_TABLE_COLUMNS)
        finally:
            client.close()
