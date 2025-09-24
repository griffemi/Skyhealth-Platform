from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession, functions as F

from pipelines.config import export_path, settings
from pipelines.validation import validate_gold
from .job import Job, frame_to_pandas, persist_partition, read_partition

SILVER_TABLE = settings.iceberg_table_identifier("silver", "climate_daily_features")
GOLD_TABLE = settings.iceberg_table_identifier("gold", "climate_daily_summary")
EXPORT_PATH = export_path("climate_daily_summary")


class GoldClimateDailySummaryJob(Job):
    """Aggregate Silver features into the curated Gold summary."""

    def run_for_date(self, spark: SparkSession, target_date: date) -> None:
        partition_date = target_date
        silver = read_partition(spark, SILVER_TABLE, partition_date)
        if silver is None:
            return
        gold = (
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
        pdf = frame_to_pandas(gold)
        if pdf is None:
            return
        validate_gold(partition_date.isoformat(), pdf)
        persist_partition(
            spark,
            gold,
            GOLD_TABLE,
            export_path=EXPORT_PATH,
            partition_cols=["ingest_date"],
        )


gold_job = GoldClimateDailySummaryJob()
