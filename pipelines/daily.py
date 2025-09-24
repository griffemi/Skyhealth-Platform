from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DailyPartitionsDefinition,
    RunRequest,
    ScheduleDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    resource,
    sensor,
)
from pyspark.sql import SparkSession, functions as F

from pipelines.assets import (
    bronze_openmeteo,
    gold_climate_daily_summary,
    silver_climate_daily_features,
)
from pipelines.config import settings
from pipelines.publish_bigquery import publish_gold_partition
from pipelines.utils.session import get_spark

__all__ = [
    "spark_session_resource",
    "PARTITIONS",
    "ASSETS",
    "DAILY_JOB",
    "HOUSEKEEPING_JOB",
    "DAILY_SCHEDULE",
    "HOUSEKEEPING_SCHEDULE",
    "bronze_partition_sensor",
]


PARTITIONS = DailyPartitionsDefinition(start_date=datetime(2024, 1, 1))


@dataclass(frozen=True)
class IcebergTables:
    bronze_fq: str
    silver_fq: str
    gold_fq: str
    bronze_name: str
    silver_name: str
    gold_name: str


TABLES = IcebergTables(
    bronze_fq=settings.iceberg_table_identifier("bronze", "openmeteo_daily"),
    silver_fq=settings.iceberg_table_identifier("silver", "climate_daily_features"),
    gold_fq=settings.iceberg_table_identifier("gold", "climate_daily_summary"),
    bronze_name=settings.iceberg_table_name("bronze", "openmeteo_daily"),
    silver_name=settings.iceberg_table_name("silver", "climate_daily_features"),
    gold_name=settings.iceberg_table_name("gold", "climate_daily_summary"),
)


@resource
def spark_session_resource(_context):
    spark = get_spark("skyhealth-dagster")
    try:
        yield spark
    finally:
        spark.stop()


def _partition_key(context: AssetExecutionContext) -> date:
    if not context.has_partition_key:
        raise RuntimeError(
            f"Asset '{context.asset_key.to_user_string()}' executed without a partition key"
        )
    return date.fromisoformat(context.partition_key)

@asset(
    partitions_def=PARTITIONS,
    compute_kind="spark",
    required_resource_keys={"spark_session"},
)
def bronze_openmeteo_daily(context: AssetExecutionContext) -> None:
    spark = context.resources.spark_session
    partition_date = _partition_key(context)
    context.log.info("Ingesting Open-Meteo data for %s", partition_date.isoformat())
    bronze_openmeteo.bronze_job.run_for_date(
        spark, partition_date
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[bronze_openmeteo_daily],
    compute_kind="spark",
    required_resource_keys={"spark_session"},
)
def silver_climate_daily_features_asset(context: AssetExecutionContext) -> None:
    partition_date = _partition_key(context)
    context.log.info("Building silver features for %s", partition_date.isoformat())
    
    silver_climate_daily_features.silver_job.run_for_date(
        context.resources.spark_session, partition_date
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[silver_climate_daily_features_asset],
    compute_kind="spark",
    required_resource_keys={"spark_session"},
)
def gold_climate_daily_summary_asset(context: AssetExecutionContext) -> None:
    partition_date = _partition_key(context)
    context.log.info("Building gold summary for %s", partition_date.isoformat())
    
    gold_climate_daily_summary.gold_job.run_for_date(
        context.resources.spark_session, partition_date
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[gold_climate_daily_summary_asset],
    compute_kind="analytics",
    required_resource_keys={"spark_session"},
)
def publish_gold_partition_asset(context: AssetExecutionContext) -> None:
    partition_date = _partition_key(context)
    context.log.info("Publishing gold summary for %s", partition_date.isoformat())
    
    publish_gold_partition(
        context.resources.spark_session, partition_date
    )


@asset(
    name="iceberg_housekeeping",
    compute_kind="spark",
    description="Expire Iceberg snapshots and remove orphan files.",
    required_resource_keys={"spark_session"},
)
def iceberg_housekeeping(context: AssetExecutionContext) -> None:
    spark = context.resources.spark_session
    catalog = settings.iceberg_catalog
    horizon = datetime.now(timezone.utc) - timedelta(hours=168)
    cutoff_literal = horizon.strftime("%Y-%m-%d %H:%M:%S")

    for table in (TABLES.bronze_name, TABLES.silver_name, TABLES.gold_name):
        spark.sql(f"CALL {catalog}.system.expire_snapshots('{table}', TIMESTAMP '{cutoff_literal}')")
        spark.sql(f"CALL {catalog}.system.remove_orphan_files('{table}', TIMESTAMP '{cutoff_literal}')")


ASSETS = [
    bronze_openmeteo_daily,
    silver_climate_daily_features_asset,
    gold_climate_daily_summary_asset,
    publish_gold_partition_asset,
    iceberg_housekeeping,
]


DAILY_JOB = define_asset_job(
    "daily_partition_job",
    selection=AssetSelection.assets(
        bronze_openmeteo_daily,
        silver_climate_daily_features_asset,
        gold_climate_daily_summary_asset,
        publish_gold_partition_asset,
    ),
    partitions_def=PARTITIONS,
)

HOUSEKEEPING_JOB = define_asset_job(
    "weekly_housekeeping_job", selection=AssetSelection.assets(iceberg_housekeeping)
)

DAILY_SCHEDULE = build_schedule_from_partitioned_job(
    job=DAILY_JOB, minute_of_hour=0, hour_of_day=3, name="daily_iceberg_schedule"
)

HOUSEKEEPING_SCHEDULE = ScheduleDefinition(
    name="weekly_iceberg_housekeeping", cron_schedule="0 6 * * MON", job=HOUSEKEEPING_JOB
)

def _bronze_partition_missing(target_date: date) -> bool:
    spark = get_spark("bronze_sensor")
    try:
        exists = (
            spark.read.table(TABLES.bronze_fq)
            .where(F.col("observation_date") == F.lit(target_date))
            .limit(1)
            .collect()
        )
        return not bool(exists)
    finally:
        spark.stop()

@sensor(job=DAILY_JOB, minimum_interval_seconds=3600)
def bronze_partition_sensor(context):
    target_date = date.today() - timedelta(days=1)
    cursor_value = target_date.isoformat()
    if context.cursor == cursor_value:
        return
    if _bronze_partition_missing(target_date):
        context.update_cursor(cursor_value)
        yield RunRequest(
            run_key=f"bronze-missing-{cursor_value}",
            partition_key=cursor_value,
        )
