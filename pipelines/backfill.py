from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Sequence

from dagster import AssetExecutionContext, Field, job, op
from pyspark.sql import SparkSession

from pipelines.config import settings
from pipelines.publish_bigquery import publish_gold_partition
from .daily import spark_session_resource
from .jobs import bronze_openmeteo, gold_climate_daily_summary, silver_climate_daily_features

__all__ = [
    "BackfillConfig",
    "BACKFILL_CONFIG_SCHEMA",
    "climate_backfill_job",
]


@dataclass(frozen=True)
class BackfillConfig:
    start: date
    end: date
    location_set: str
    locations: Sequence[bronze_openmeteo.Location]

    @property
    def window(self) -> tuple[date, date]:
        return self.start, self.end

    @classmethod
    def from_mapping(cls, mapping: dict) -> "BackfillConfig":
        start = date.fromisoformat(mapping["start"])
        end_value = mapping.get("end")
        end = date.fromisoformat(end_value) if end_value else start
        if end < start:
            raise ValueError("End date must be on or after the start date.")
        location_set = mapping.get("location_set") or settings.locations_file
        locations = bronze_openmeteo.load_locations(location_set)
        return cls(start=start, end=end, location_set=location_set, locations=locations)


BACKFILL_CONFIG_SCHEMA = {
    "start": Field(str, description="Inclusive start date (YYYY-MM-DD)."),
    "end": Field(
        str,
        is_required=False,
        description="Inclusive end date (YYYY-MM-DD). Defaults to the start date.",
    ),
    "location_set": Field(
        str,
        default_value=settings.locations_file,
        description="CSV file containing location definitions.",
        is_required=False,
    ),
}


def _run_with_window(
    context: AssetExecutionContext,
    window: tuple[date, date],
    fn: Callable[[SparkSession, tuple[date, date]], object | None],
    *,
    log_message: str | None = None,
) -> object | None:
    spark = context.resources.spark_session
    if log_message:
        start_date, end_date = window
        context.log.info(log_message, f"{start_date.isoformat()}→{end_date.isoformat()}")
    return fn(spark, window)


@op(config_schema=BACKFILL_CONFIG_SCHEMA)
def prepare_backfill(context) -> BackfillConfig:
    return BackfillConfig.from_mapping(context.op_config)


@op(required_resource_keys={"spark_session"})
def bronze_backfill(context, config: BackfillConfig) -> BackfillConfig:
    def materialize_bronze_window(spark: SparkSession, window: tuple[date, date]) -> None:
        start_date, end_date = window
        bronze_openmeteo.bronze_job.run_range(
            spark, start_date, end_date
        )

    _run_with_window(
        context,
        config.window,
        materialize_bronze_window,
        log_message="Backfilling Bronze window %s",
    )
    return config


@op(required_resource_keys={"spark_session"})
def silver_backfill(context, config: BackfillConfig) -> BackfillConfig:
    def materialize_silver_window(
        spark: SparkSession, window: tuple[date, date]
    ) -> None:
        start_date, end_date = window
        silver_climate_daily_features.silver_job.run_range(
            spark, start_date, end_date
        )

    _run_with_window(
        context,
        config.window,
        materialize_silver_window,
        log_message="Backfilling Silver window %s",
    )
    return config


@op(required_resource_keys={"spark_session"})
def gold_backfill(context, config: BackfillConfig) -> BackfillConfig:
    def materialize_gold_window(spark: SparkSession, window: tuple[date, date]) -> None:
        start_date, end_date = window
        gold_climate_daily_summary.gold_job.run_range(spark, start_date, end_date)

    _run_with_window(
        context,
        config.window,
        materialize_gold_window,
        log_message="Backfilling Gold window %s",
    )
    return config


@op(required_resource_keys={"spark_session"})
def publish_backfill(context, config: BackfillConfig) -> None:
    def publish_gold_window(spark: SparkSession, window: tuple[date, date]) -> None:
        start_date, end_date = window
        for partition_date in bronze_openmeteo.bronze_job.iter_dates(
            start_date, end_date
        ):
            publish_gold_partition(spark, partition_date)

    _run_with_window(
        context,
        config.window,
        publish_gold_window,
        log_message="Publishing Gold window %s",
    )


@job(resource_defs={"spark_session": spark_session_resource})
def climate_backfill_job():
    publish_backfill(gold_backfill(silver_backfill(bronze_backfill(prepare_backfill()))))
