from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

BRONZE_DAILY_SCHEMA = StructType([
    StructField("location_id", StringType(), False),
    StructField("d", DateType(), False),
    StructField("tavg_c", DoubleType(), True),
    StructField("prcp_mm", DoubleType(), True),
])

SILVER_DAILY_SCHEMA = StructType(BRONZE_DAILY_SCHEMA.fields + [
    StructField("hdd18", DoubleType(), True),
    StructField("cdd18", DoubleType(), True),
    StructField("gdd10_30", DoubleType(), True),
    StructField("prcp_flag", StringType(), True),
])
