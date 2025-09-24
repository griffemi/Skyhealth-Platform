from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame


def merge_iceberg_table(
    df: DataFrame,
    table_identifier: str,
) -> None:
    """Write records into an Iceberg table using the DataFrameWriterV2 API.

    Iceberg handles upserts by overwriting the partitions present in ``df``. When the
    table does not exist yet we create (or replace) it using the incoming schema.
    """
    spark = df.sparkSession
    writer = df.writeTo(table_identifier).using("iceberg")

    if spark.catalog.tableExists(table_identifier):
        writer.overwritePartitions()
    else:
        writer.create()
