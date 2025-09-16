from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Iterator

from pyspark.sql import DataFrame, SparkSession, functions as F

from skyhealth.config import export_path, settings
from skyhealth.validation import validate_silver
from spark.jobs.utils.cli import base_parser
from spark.jobs.utils.io import merge_delta_table
from spark.jobs.utils.log import configure
from spark.jobs.utils.session import get_spark

BRONZE_TABLE = settings.delta_table_uri("bronze", "openmeteo_daily")
SILVER_TABLE = settings.delta_table_uri("silver", "climate_daily_features")
EXPORT_PATH = export_path("climate_daily_features")


def iter_dates(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def read_bronze_partition(spark: SparkSession, target_date: date) -> DataFrame | None:
    bronze = (
        spark.read.format("delta").load(BRONZE_TABLE).where(F.col("observation_date") == F.lit(target_date))
    )
    if bronze.head(1):
        return bronze
    return None


def transform_features(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        .withColumn("hdd18", F.greatest(F.lit(0.0), F.lit(18.0) - F.col("tavg_c")))
        .withColumn("cdd18", F.greatest(F.lit(0.0), F.col("tavg_c") - F.lit(18.0)))
        .withColumn(
            "gdd10_30",
            F.greatest(
                F.lit(0.0),
                F.least(F.lit(30.0), F.greatest(F.col("tavg_c"), F.lit(10.0))) - F.lit(10.0),
            ),
        )
        .withColumn("prcp_flag", F.when(F.col("prcp_mm") > 0, F.lit("Y")).otherwise(F.lit("N")))
        .select(
            "location_id",
            "source",
            "observation_date",
            "ingest_date",
            "tavg_c",
            "prcp_mm",
            "latitude",
            "longitude",
            "hdd18",
            "cdd18",
            "gdd10_30",
            "prcp_flag",
            "ingested_at",
        )
    )


def validate_and_write(spark: SparkSession, features: DataFrame, target_date: date) -> None:
    pdf = features.toPandas()
    if pdf.empty:
        return
    validate_silver(target_date.isoformat(), pdf)
    merge_delta_table(
        spark,
        features,
        SILVER_TABLE,
        ["location_id", "observation_date"],
        partition_cols=["ingest_date"],
    )
    features.write.mode("overwrite").partitionBy("ingest_date").parquet(EXPORT_PATH)


def process_dates(spark: SparkSession, dates: Iterable[date]) -> None:
    for target_date in dates:
        bronze = read_bronze_partition(spark, target_date)
        if bronze is None:
            continue
        features = transform_features(bronze)
        validate_and_write(spark, features, target_date)


def run_range(spark: SparkSession, start: date, end: date) -> None:
    process_dates(spark, iter_dates(start, end))


def main() -> None:
    configure()
    parser = base_parser("Silver climate features")
    parser.add_argument("--date", help="Target partition date (YYYY-MM-DD)")
    parser.add_argument("--range-start")
    parser.add_argument("--range-end")
    args = parser.parse_args()

    spark = get_spark("silver_climate_daily_features", warehouse_uri=settings.bucket_uri("silver"))
    if args.date:
        target = date.fromisoformat(args.date)
        run_range(spark, target, target)
    else:
        start = date.fromisoformat(args.range_start) if args.range_start else date.today()
        end = date.fromisoformat(args.range_end) if args.range_end else start
        run_range(spark, start, end)
    spark.stop()


if __name__ == "__main__":
    main()
