from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_path: str,
    match_keys: Sequence[str],
    partition_cols: Sequence[str] | None = None,
) -> None:
    """Upsert a DataFrame into a Delta table using the provided match keys."""
    if DeltaTable.isDeltaTable(spark, table_path):
        condition = " AND ".join(f"target.{key} = src.{key}" for key in match_keys)
        (
            DeltaTable.forPath(spark, table_path)
            .alias("target")
            .merge(df.alias("src"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(table_path)
