from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from skyhealth.config import settings
from spark.jobs.utils.cli import base_parser
from spark.jobs.utils.io import merge_iceberg_table
from spark.jobs.utils.log import configure
from spark.jobs.utils.session import get_spark

API_URL = "https://api.open-meteo.com/v1/forecast"
BRONZE_TABLE = settings.iceberg_table_identifier("bronze", "openmeteo_daily")

SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("location_id", StringType(), False),
        StructField("observation_date", DateType(), False),
        StructField("ingest_date", DateType(), False),
        StructField("tavg_c", DoubleType(), True),
        StructField("prcp_mm", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
)


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


def load_locations(path: str | Path) -> list[Location]:
    with Path(path).open("r", encoding="utf-8") as handle:
        lines = handle.readlines()[1:]
    return [
        Location(location_id=row[0], latitude=float(row[1]), longitude=float(row[2]))
        for row in (line.strip().split(",") for line in lines if line.strip())
    ]


def iter_dates(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)



def _coerce_locations(locations: Sequence[Location | dict]) -> list[Location]:
    coerced: list[Location] = []
    for loc in locations:
        if isinstance(loc, Location):
            coerced.append(loc)
        else:
            coerced.append(
                Location(
                    location_id=str(loc["location_id"]),
                    latitude=float(loc["latitude"]),
                    longitude=float(loc["longitude"]),
                )
            )
    return coerced



def build_rows(client: OpenMeteoClient, locations: Sequence[Location], target_date: date) -> list[dict]:
    observed_at = datetime.now(timezone.utc)
    records: list[dict] = []
    for loc in locations:
        readings = client.fetch_daily(loc, target_date)
        records.append(
            {
                "source": "openmeteo",
                "location_id": loc.location_id,
                "observation_date": target_date,
                "ingest_date": target_date,
                "tavg_c": readings["tavg_c"],
                "prcp_mm": readings["prcp_mm"],
                "latitude": loc.latitude,
                "longitude": loc.longitude,
                "ingested_at": observed_at,
            }
        )
    return records


def materialize_dates(
    spark: SparkSession,
    client: OpenMeteoClient,
    locations: Sequence[Location | dict],
    dates: Iterable[date],
) -> None:
    coerced = _coerce_locations(locations)
    for target_date in dates:
        rows = build_rows(client, coerced, target_date)
        if not rows:
            continue
        df = spark.createDataFrame(rows, schema=SCHEMA)
        merge_iceberg_table(
            spark,
            df,
            BRONZE_TABLE,
            ["location_id", "observation_date"],
            partition_cols=["ingest_date"],
        )


def run_backfill(spark: SparkSession, locations: Sequence[Location | dict], start: date, end: date) -> None:
    client = OpenMeteoClient()
    materialize_dates(spark, client, locations, iter_dates(start, end))


def run_incremental(spark: SparkSession, locations: Sequence[Location | dict], lookback_days: int) -> None:
    today = date.today()
    start = today - timedelta(days=lookback_days)
    client = OpenMeteoClient()
    materialize_dates(spark, client, locations, iter_dates(start, today))




def main() -> None:
    configure()
    parser = base_parser("Bronze Open-Meteo")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="backfill")
    parser.add_argument("--range-start")
    parser.add_argument("--range-end")
    parser.add_argument("--lookback", type=int, default=0)
    parser.add_argument("--location-set", default=settings.locations_file)
    args = parser.parse_args()

    locations = load_locations(args.location_set)
    spark = get_spark("bronze_openmeteo")

    if args.mode == "backfill":
        if not args.range_start:
            raise ValueError("--range-start is required for backfill mode")
        start = date.fromisoformat(args.range_start)
        end = date.fromisoformat(args.range_end) if args.range_end else date.today()
        run_backfill(spark, locations, start, end)
    else:
        run_incremental(spark, locations, args.lookback)

    spark.stop()


if __name__ == "__main__":
    main()
