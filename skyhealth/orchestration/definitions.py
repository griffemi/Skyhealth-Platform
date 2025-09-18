from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DailyPartitionsDefinition,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    sensor,
)
from pyspark.sql import functions as F

from skyhealth.config import settings
from skyhealth.publish_bigquery import publish_gold_partition
from spark.jobs import bronze_openmeteo, gold_climate_daily_summary, silver_climate_daily_features
from spark.jobs.utils.session import get_spark

PARTITIONS = DailyPartitionsDefinition(start_date=date(2024, 1, 1))
BRONZE_TABLE = settings.iceberg_table_identifier("bronze", "openmeteo_daily")
SILVER_TABLE = settings.iceberg_table_identifier("silver", "climate_daily_features")
GOLD_TABLE = settings.iceberg_table_identifier("gold", "climate_daily_summary")

BRONZE_NAME = settings.iceberg_table_name("bronze", "openmeteo_daily")
SILVER_NAME = settings.iceberg_table_name("silver", "climate_daily_features")
GOLD_NAME = settings.iceberg_table_name("gold", "climate_daily_summary")


def _partition_key(context: AssetExecutionContext) -> date:
    if not context.has_partition_key:
        raise RuntimeError("Asset executed without a partition key")
    return date.fromisoformat(context.partition_key)


@contextmanager
def spark_session(app_name: str, layer: str | None = None):
    # Layer retained for backwards compatibility; Iceberg uses a shared warehouse.
    spark = get_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()


@asset(partitions_def=PARTITIONS, compute_kind="spark")
def bronze_openmeteo_daily(context: AssetExecutionContext) -> None:
    target_date = _partition_key(context)
    locations = bronze_openmeteo.load_locations(settings.locations_file)
    context.log.info("Ingesting Open-Meteo data for %s", target_date.isoformat())
    with spark_session("bronze_openmeteo", layer="bronze") as spark:
        bronze_openmeteo.run_backfill(spark, locations, target_date, target_date)


@asset(partitions_def=PARTITIONS, deps=[bronze_openmeteo_daily], compute_kind="spark")
def silver_climate_daily_features_asset(context: AssetExecutionContext) -> None:
    target_date = _partition_key(context)
    context.log.info("Building silver features for %s", target_date.isoformat())
    with spark_session("silver_climate_features", layer="silver") as spark:
        silver_climate_daily_features.run_range(spark, target_date, target_date)


@asset(partitions_def=PARTITIONS, deps=[silver_climate_daily_features_asset], compute_kind="spark")
def gold_climate_daily_summary_asset(context: AssetExecutionContext) -> None:
    target_date = _partition_key(context)
    context.log.info("Building gold summary for %s", target_date.isoformat())
    with spark_session("gold_daily_summary", layer="gold") as spark:
        gold_climate_daily_summary.run_range(spark, target_date, target_date)


@asset(
    partitions_def=PARTITIONS,
    deps=[gold_climate_daily_summary_asset],
    compute_kind="bigquery",
)
def publish_gold_to_bigquery(context: AssetExecutionContext) -> None:
    target_date = _partition_key(context)
    context.log.info("Publishing gold summary for %s to BigQuery", target_date.isoformat())
    with spark_session("publish_gold", layer="gold") as spark:
        publish_gold_partition(spark, target_date)


@asset(
    name="iceberg_housekeeping",
    compute_kind="spark",
    description="Expire Iceberg snapshots and remove orphan files.",
)
def iceberg_housekeeping() -> None:
    with spark_session("iceberg_housekeeping") as spark:
        catalog = settings.iceberg_catalog
        horizon = datetime.now(timezone.utc) - timedelta(hours=168)
        horizon_literal = horizon.strftime("%Y-%m-%d %H:%M:%S")
        tables = [BRONZE_NAME, SILVER_NAME, GOLD_NAME]
        for table in tables:
            spark.sql(
                f"CALL {catalog}.system.expire_snapshots('{table}', TIMESTAMP '{horizon_literal}')"
            )
            spark.sql(
                f"CALL {catalog}.system.remove_orphan_files('{table}', TIMESTAMP '{horizon_literal}')"
            )


daily_job = define_asset_job(
    "daily_partition_job",
    selection=AssetSelection.assets(
        bronze_openmeteo_daily,
        silver_climate_daily_features_asset,
        gold_climate_daily_summary_asset,
        publish_gold_to_bigquery,
    ),
    partitions_def=PARTITIONS,
)

housekeeping_job = define_asset_job(
    "weekly_housekeeping_job", selection=AssetSelection.assets(iceberg_housekeeping)
)

daily_schedule = build_schedule_from_partitioned_job(
    job=daily_job, minute=0, hour=3, name="daily_iceberg_schedule"
)

housekeeping_schedule = ScheduleDefinition(
    name="weekly_iceberg_housekeeping", cron_schedule="0 6 * * MON", job=housekeeping_job
)


@sensor(job=daily_job, minimum_interval_seconds=3600)
def bronze_partition_sensor(context):
    target_date = date.today() - timedelta(days=1)
    if context.cursor == target_date.isoformat():
        return
    with spark_session("bronze_sensor", layer="bronze") as spark:
        exists = (
            spark.read.table(BRONZE_TABLE)
            .where(F.col("observation_date") == F.lit(target_date))
            .limit(1)
            .collect()
        )
    if not exists:
        context.update_cursor(target_date.isoformat())
        yield RunRequest(
            run_key=f"bronze-missing-{target_date.isoformat()}",
            partition_key=target_date.isoformat(),
        )


defs = Definitions(
    assets=[
        bronze_openmeteo_daily,
        silver_climate_daily_features_asset,
        gold_climate_daily_summary_asset,
        publish_gold_to_bigquery,
        iceberg_housekeeping,
    ],
    schedules=[daily_schedule, housekeeping_schedule],
    sensors=[bronze_partition_sensor],
)
