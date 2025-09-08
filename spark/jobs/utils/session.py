from pyspark.sql import SparkSession

from skyhealth.config import settings


def get_spark(app_name: str = "skyhealth", warehouse_uri: str | None = None) -> SparkSession:
    warehouse = warehouse_uri or settings.warehouse_uri
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
    )
    if warehouse.startswith("gs://"):
        builder = builder.config(
            "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
    return builder.getOrCreate()
