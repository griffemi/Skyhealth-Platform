from pyspark.sql import SparkSession

from pipelines.config import settings

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"


def get_spark(app_name: str = "skyhealth", warehouse_uri: str | None = None) -> SparkSession:
    warehouse = warehouse_uri or settings.iceberg_warehouse()
    catalog = settings.iceberg_catalog
    builder = (
        SparkSession.Builder().appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.jars.packages", ICEBERG_PACKAGE)
    )
    if warehouse.startswith("gs://"):
        builder = builder.config(
            "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        ).config(
            "spark.hadoop.google.cloud.auth.service.account.enable", "true"
        )
    return builder.getOrCreate()
