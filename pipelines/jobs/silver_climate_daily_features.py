from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession, functions as F

from pipelines.config import export_path, settings
from pipelines.validation import validate_silver
from .job import Job, frame_to_pandas, persist_partition, read_partition

BRONZE_TABLE = settings.iceberg_table_identifier("bronze", "openmeteo_daily")
SILVER_TABLE = settings.iceberg_table_identifier("silver", "climate_daily_features")
EXPORT_PATH = export_path("climate_daily_features")


class SilverClimateDailyFeaturesJob(Job):
    """Transform Bronze partitions into Silver analytical features."""

    def run_for_date(self, spark: SparkSession, target_date: date) -> None:
        partition_date = target_date
        bronze = read_partition(spark, BRONZE_TABLE, partition_date)
        if bronze is None:
            return
        features = (
            bronze
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
        pdf = frame_to_pandas(features)
        if pdf is None:
            return
        validate_silver(partition_date.isoformat(), pdf)
        persist_partition(
            spark,
            features,
            SILVER_TABLE,
            export_path=EXPORT_PATH,
            partition_cols=["ingest_date"],
        )


silver_job = SilverClimateDailyFeaturesJob()
