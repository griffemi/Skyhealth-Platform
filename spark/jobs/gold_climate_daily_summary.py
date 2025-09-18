from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Iterator

from pyspark.sql import DataFrame, SparkSession, functions as F

from skyhealth.config import export_path, settings
from skyhealth.validation import validate_gold
from spark.jobs.utils.cli import base_parser
from spark.jobs.utils.io import merge_iceberg_table
from spark.jobs.utils.log import configure
from spark.jobs.utils.session import get_spark

SILVER_TABLE = settings.iceberg_table_identifier("silver", "climate_daily_features")
GOLD_TABLE = settings.iceberg_table_identifier("gold", "climate_daily_summary")
EXPORT_PATH = export_path("climate_daily_summary")


def iter_dates(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def read_silver_partition(spark: SparkSession, target_date: date) -> DataFrame | None:
    silver = spark.read.table(SILVER_TABLE).where(F.col("observation_date") == F.lit(target_date))
    if silver.head(1):
        return silver
    return None


def aggregate_gold(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy("location_id", "observation_date", "ingest_date")
        .agg(
            F.first("source").alias("source"),
            F.avg("tavg_c").alias("avg_temp_c"),
            F.sum("prcp_mm").alias("total_prcp_mm"),
            F.max("prcp_flag").alias("had_precip"),
            F.avg("hdd18").alias("avg_hdd18"),
            F.avg("cdd18").alias("avg_cdd18"),
        )
        .withColumnRenamed("location_id", "region_id")
    )


def validate_and_write(spark: SparkSession, gold: DataFrame, target_date: date) -> None:
    pdf = gold.toPandas()
    if pdf.empty:
        return
    validate_gold(target_date.isoformat(), pdf)
    merge_iceberg_table(
        spark,
        gold,
        GOLD_TABLE,
        ["region_id", "observation_date"],
        partition_cols=["ingest_date"],
    )
    gold.write.mode("overwrite").partitionBy("ingest_date").parquet(EXPORT_PATH)


def process_dates(spark: SparkSession, dates: Iterable[date]) -> None:
    for target_date in dates:
        silver = read_silver_partition(spark, target_date)
        if silver is None:
            continue
        gold = aggregate_gold(silver)
        validate_and_write(spark, gold, target_date)


def run_range(spark: SparkSession, start: date, end: date) -> None:
    process_dates(spark, iter_dates(start, end))


def main() -> None:
    configure()
    parser = base_parser("Gold climate summary")
    parser.add_argument("--date", help="Target partition date (YYYY-MM-DD)")
    parser.add_argument("--range-start")
    parser.add_argument("--range-end")
    args = parser.parse_args()

    spark = get_spark("gold_climate_daily_summary")
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
