from __future__ import annotations

from collections.abc import Sequence
from uuid import uuid4

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

    temp_view = f"merge_src_{uuid4().hex}"
    df.createOrReplaceTempView(temp_view)
    try:
        condition = " AND ".join(f"target.{key} = src.{key}" for key in match_keys)
        assignments = ", ".join(f"target.{col} = src.{col}" for col in df.columns)
        insert_cols = ", ".join(df.columns)
        insert_values = ", ".join(f"src.{col}" for col in df.columns)
        spark.sql(
            f"""
            MERGE INTO {table_identifier} AS target
            USING {temp_view} AS src
            ON {condition}
            WHEN MATCHED THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)
