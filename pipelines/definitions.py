from __future__ import annotations

from dagster import Definitions

from .backfill import climate_backfill_job
from .daily import (
    ASSETS,
    DAILY_JOB,
    DAILY_SCHEDULE,
    HOUSEKEEPING_JOB,
    HOUSEKEEPING_SCHEDULE,
    bronze_partition_sensor,
    spark_session_resource,
)

__all__ = ["defs"]


defs = Definitions(
    assets=ASSETS,
    jobs=[DAILY_JOB, HOUSEKEEPING_JOB, climate_backfill_job],
    schedules=[DAILY_SCHEDULE, HOUSEKEEPING_SCHEDULE],
    sensors=[bronze_partition_sensor],
    resources={"spark_session": spark_session_resource},
)
