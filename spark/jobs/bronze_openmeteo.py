from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

from spark.jobs.utils.cli import base_parser
from spark.jobs.utils.log import configure
from spark.jobs.utils.session import get_spark

SCHEMA = StructType([
    StructField("location_id", StringType()),
    StructField("d", DateType()),
    StructField("tavg_c", DoubleType()),
    StructField("prcp_mm", DoubleType()),
])

API_URL = "https://api.open-meteo.com/v1/forecast"


def _load_locations(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f.readlines()[1:]:
            location_id, lat, lon = line.strip().split(",")
            yield {"location_id": location_id, "latitude": lat, "longitude": lon}


def fetch_day(lat: str, lon: str, day: datetime) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": day.strftime("%Y-%m-%d"),
        "end_date": day.strftime("%Y-%m-%d"),
        "daily": ["temperature_2m_mean", "precipitation_sum"],
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()["daily"]
    return {
        "tavg_c": data["temperature_2m_mean"][0],
        "prcp_mm": data["precipitation_sum"][0],
    }


def run_backfill(spark: SparkSession, locations: Iterable[dict], start: datetime, end: datetime) -> None:
    rows = []
    cur = start
    while cur <= end:
        for loc in locations:
            vals = fetch_day(loc["latitude"], loc["longitude"], cur)
            rows.append({"location_id": loc["location_id"], "d": cur.date(), **vals})
        cur += timedelta(days=1)
    if rows:
        df = spark.createDataFrame(rows, schema=SCHEMA)
        df.writeTo("local.climate_bronze_daily").append()


def run_incremental(spark: SparkSession, locations: Iterable[dict], lookback: int) -> None:
    today = datetime.utcnow().date()
    start = today - timedelta(days=lookback)
    cur = start
    while cur <= today:
        rows = []
        for loc in locations:
            vals = fetch_day(loc["latitude"], loc["longitude"], cur)
            rows.append({"location_id": loc["location_id"], "d": cur, **vals})
        if rows:
            df = spark.createDataFrame(rows, schema=SCHEMA)
            df.writeTo("local.climate_bronze_daily").append()
        cur += timedelta(days=1)


def main() -> None:
    configure()
    parser = base_parser("Bronze Open-Meteo")
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="backfill")
    parser.add_argument("--range-start")
    parser.add_argument("--range-end")
    parser.add_argument("--lookback", type=int, default=0)
    parser.add_argument("--location-set", default="data/locations.csv")
    args = parser.parse_args()

    locations = list(_load_locations(args.location_set))
    spark = get_spark("bronze_openmeteo", warehouse_uri=args.warehouse_uri)
    if args.mode == "backfill":
        start = datetime.fromisoformat(args.range_start)
        end = datetime.fromisoformat(args.range_end) if args.range_end else datetime.utcnow()
        run_backfill(spark, locations, start, end)
    else:
        run_incremental(spark, locations, args.lookback)
    spark.stop()


if __name__ == "__main__":
    main()
