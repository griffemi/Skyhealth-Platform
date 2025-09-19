from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Any

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


def _partition_key(context: AssetExecutionContext) -> date:
    if not context.has_partition_key:
        raise RuntimeError(
            f"Asset '{context.asset_key.to_user_string()}' executed without a partition key"
        )
    return date.fromisoformat(context.partition_key)


@contextmanager
def spark_session(app_name: str, layer: str | None = None):
    # Layer retained for backwards compatibility; Iceberg uses a shared warehouse.
    spark = get_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()


def _run_spark_job(app_name: str, job: Callable[[Any], None], *, layer: str | None = None) -> None:
    with spark_session(app_name, layer=layer) as spark:
        job(spark)


def _run_partition_job(
    context: AssetExecutionContext,
    *,
    app_name: str,
    runner: Callable[[Any, date], None],
    layer: str | None,
    logger_message: str,
) -> None:
    target_date = _partition_key(context)
    context.log.info(logger_message, target_date.isoformat())

    _run_spark_job(app_name, lambda spark: runner(spark, target_date), layer=layer)


def _bronze_partition_missing(target_date: date) -> bool:
    found: list[bool] = []

    def _check(spark):
        exists = (
            spark.read.table(TABLES.bronze_fq)
            .where(F.col("observation_date") == F.lit(target_date))
            .limit(1)
            .collect()
        )
        found.append(bool(exists))

    _run_spark_job("bronze_sensor", _check, layer="bronze")
    return not found or not found[0]


@asset(partitions_def=PARTITIONS, compute_kind="spark")
def bronze_openmeteo_daily(context: AssetExecutionContext) -> None:
    """Fetch Open-Meteo observations and land them in the Bronze Iceberg table."""

    locations = bronze_openmeteo.load_locations(settings.locations_file)
    _run_partition_job(
        context,
        app_name="bronze_openmeteo",
        layer="bronze",
        logger_message="Ingesting Open-Meteo data for %s",
        runner=lambda spark, date_: bronze_openmeteo.run_backfill(spark, locations, date_, date_),
    )


@asset(partitions_def=PARTITIONS, deps=[bronze_openmeteo_daily], compute_kind="spark")
def silver_climate_daily_features_asset(context: AssetExecutionContext) -> None:
    """Transform Bronze observations into Silver analytical features."""

    _run_partition_job(
        context,
        app_name="silver_climate_features",
        layer="silver",
        logger_message="Building silver features for %s",
        runner=lambda spark, date_: silver_climate_daily_features.run_range(spark, date_, date_),
    )


@asset(partitions_def=PARTITIONS, deps=[silver_climate_daily_features_asset], compute_kind="spark")
def gold_climate_daily_summary_asset(context: AssetExecutionContext) -> None:
    """Aggregate Silver features into the curated Gold summary."""

    _run_partition_job(
        context,
        app_name="gold_daily_summary",
        layer="gold",
        logger_message="Building gold summary for %s",
        runner=lambda spark, date_: gold_climate_daily_summary.run_range(spark, date_, date_),
    )


@asset(
    partitions_def=PARTITIONS,
    deps=[gold_climate_daily_summary_asset],
    compute_kind="analytics",
)
def publish_gold_partition_asset(context: AssetExecutionContext) -> None:
    """Publish the Gold partition to the downstream analytics store."""
    _run_partition_job(
        context,
        app_name="publish_gold",
        layer="gold",
        logger_message="Publishing gold summary for %s",
        runner=lambda spark, date_: publish_gold_partition(spark, date_),
    )


@asset(
    name="iceberg_housekeeping",
    compute_kind="spark",
    description="Expire Iceberg snapshots and remove orphan files.",
)
def iceberg_housekeeping() -> None:
    """Expire stale Iceberg snapshots and remove orphaned files."""

    def _run_housekeeping(spark):
        catalog = settings.iceberg_catalog
        horizon = datetime.now(timezone.utc) - timedelta(hours=168)
        cutoff_literal = horizon.strftime("%Y-%m-%d %H:%M:%S")
        for table in (TABLES.bronze_name, TABLES.silver_name, TABLES.gold_name):
            spark.sql(
                f"CALL {catalog}.system.expire_snapshots('{table}', TIMESTAMP '{cutoff_literal}')"
            )
            spark.sql(
                f"CALL {catalog}.system.remove_orphan_files('{table}', TIMESTAMP '{cutoff_literal}')"
            )

    _run_spark_job("iceberg_housekeeping", _run_housekeeping)


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
    job=DAILY_JOB, minute=0, hour=3, name="daily_iceberg_schedule"
)

HOUSEKEEPING_SCHEDULE = ScheduleDefinition(
    name="weekly_iceberg_housekeeping", cron_schedule="0 6 * * MON", job=HOUSEKEEPING_JOB
)


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


defs = Definitions(
    assets=[
        bronze_openmeteo_daily,
        silver_climate_daily_features_asset,
        gold_climate_daily_summary_asset,
        publish_gold_partition_asset,
        iceberg_housekeeping,
    ],
    schedules=[DAILY_SCHEDULE, HOUSEKEEPING_SCHEDULE],
    sensors=[bronze_partition_sensor],
)
