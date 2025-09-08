from pyspark.sql import DataFrame

def write_iceberg(df: DataFrame, table: str, mode: str = "append") -> None:
    writer = df.writeTo(f"local.{table}")
    if mode == "overwrite":
        writer.overwritePartitions()
    else:
        writer.append()
