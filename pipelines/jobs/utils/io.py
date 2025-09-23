from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def merge_iceberg_table(
    spark: SparkSession,
    df: DataFrame,
    table_identifier: str,
    match_keys: Sequence[str],
    partition_cols: Sequence[str] | None = None,
) -> None:
    """Upsert a DataFrame into an Iceberg table using Spark MERGE semantics."""

    if not spark.catalog.tableExists(table_identifier):
        writer = df.writeTo(table_identifier).using("iceberg")
        if partition_cols:
            writer = writer.partitionedBy(*(col(name) for name in partition_cols))
        writer = writer.tableProperty("format-version", "2")
        writer.create()
        return

    condition = " AND ".join(f"target.{key} = source.{key}" for key in match_keys)
    (
        df.alias("source")
        .mergeInto(table_identifier, condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
