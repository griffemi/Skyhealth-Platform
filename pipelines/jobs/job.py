from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Iterable, Iterator, Sequence

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F

class Job(ABC):
    """Common orchestration helpers for date-partitioned Spark jobs."""

    @staticmethod
    def iter_dates(start: date, end: date) -> Iterator[date]:
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    def run(self, spark: SparkSession, dates: Iterable[date]) -> None:
        for target_date in dates:
            self.run_for_date(spark, target_date)

    @abstractmethod
    def run_for_date(self, spark: SparkSession, target_date: date) -> None:
        """Materialize one date partition."""

    def run_backfill(self, spark: SparkSession, start: date, end: date) -> None:
        self.run_range(spark, start, end)

    def run_range(self, spark: SparkSession, start: date, end: date) -> None:
        self.run(spark, self.iter_dates(start, end))

    def run_incremental(self, spark: SparkSession, lookback_days: int) -> None:
        today = date.today()
        start = today - timedelta(days=lookback_days)
        self.run(spark, self.iter_dates(start, today))


def read_partition(
    spark: SparkSession,
    table_identifier: str,
    target_date: date,
    *,
    column: str = "observation_date",
) -> DataFrame | None:
    frame = spark.read.table(table_identifier).where(F.col(column) == F.lit(target_date))
    if frame.head(1):
        return frame
    return None


def frame_to_pandas(frame: DataFrame) -> pd.DataFrame | None:
    pdf = frame.toPandas()
    return None if pdf.empty else pdf


def persist_partition(
    spark: SparkSession,
    frame: DataFrame,
    table_identifier: str,
    match_keys: Sequence[str],
    *,
    export_path: str | None = None,
    partition_cols: Sequence[str] | None = None,
) -> None:
    from .utils.io import merge_iceberg_table

    merge_iceberg_table(
        spark,
        frame,
        table_identifier,
        match_keys,
        partition_cols=partition_cols,
    )
    if export_path:
        writer = frame.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(export_path)


__all__ = [
    "Job",
    "frame_to_pandas",
    "persist_partition",
    "read_partition",
]
