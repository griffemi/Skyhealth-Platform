from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence

import requests
from pyspark.sql import Row, SparkSession
from typing_extensions import override

from pipelines.config import settings
from pipelines.assets.job import Job
from pipelines.utils.io import merge_iceberg_table
from pipelines.utils.schemas import BRONZE_OPENMETEO_SCHEMA

API_URL = "https://api.open-meteo.com/v1/forecast"
BRONZE_TABLE = settings.iceberg_table_identifier("bronze", "openmeteo_daily")

@dataclass(frozen=True)
class Location:
    location_id: str
    latitude: float
    longitude: float


class OpenMeteoClient:
    """Tiny wrapper around the Open-Meteo daily forecast endpoint."""

    def __init__(self, base_url: str = API_URL, *, session: requests.Session | None = None) -> None:
        self.base_url = base_url
        self._session = session or requests.Session()

    def fetch_daily(self, location: Location, target_date: date) -> dict:
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "start_date": target_date.strftime("%Y-%m-%d"),
            "end_date": target_date.strftime("%Y-%m-%d"),
            "daily": ["temperature_2m_mean", "precipitation_sum"],
        }
        response = self._session.get(self.base_url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()["daily"]
        return {
            "tavg_c": payload["temperature_2m_mean"][0],
            "prcp_mm": payload["precipitation_sum"][0],
        }


def load_locations(path: str | Path = "locations.csv") -> Sequence[Location]:
    with Path(path).open("r", encoding="utf-8") as handle:
        lines = handle.readlines()[1:]
    return [
        Location(location_id=row[0], latitude=float(row[1]), longitude=float(row[2]))
        for row in (line.strip().split(",") for line in lines if line.strip())
    ]


class BronzeOpenMeteoJob(Job):
    """Persist Open-Meteo observations into the Bronze Iceberg table."""

    def __init__(self, client: OpenMeteoClient | None = None) -> None:
        self._client = client or OpenMeteoClient()

    @override
    def run_for_date(
        self,
        spark: SparkSession,
        target_date: date
    ) -> None:
        partition_date = target_date
        observed_at = datetime.now(timezone.utc)
        rows = [
            Row(
                source="openmeteo",
                location_id=loc.location_id,
                observation_date=partition_date,
                ingest_date=partition_date,
                tavg_c=reading["tavg_c"],
                prcp_mm=reading["prcp_mm"],
                latitude=loc.latitude,
                longitude=loc.longitude,
                ingested_at=observed_at,
            )
            for loc in load_locations()
            for reading in [self._client.fetch_daily(loc, partition_date)]
        ]
        if not rows:
            return
        df = spark.createDataFrame(rows, schema=BRONZE_OPENMETEO_SCHEMA)
        merge_iceberg_table(df, BRONZE_TABLE )


bronze_job = BronzeOpenMeteoJob()
