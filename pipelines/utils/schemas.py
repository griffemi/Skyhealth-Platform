from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_OPENMETEO_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("location_id", StringType(), False),
        StructField("observation_date", DateType(), False),
        StructField("ingest_date", DateType(), False),
        StructField("tavg_c", DoubleType(), True),
        StructField("prcp_mm", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
)

__all__ = ["BRONZE_OPENMETEO_SCHEMA"]
