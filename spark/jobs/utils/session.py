from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from skyhealth.config import settings


def get_spark(app_name: str = "skyhealth", warehouse_uri: str | None = None) -> SparkSession:
    warehouse = warehouse_uri or settings.local_lake_root
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse)
    )
    if warehouse.startswith("gs://"):
        builder = builder.config(
            "spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        ).config(
            "spark.hadoop.google.cloud.auth.service.account.enable", "true"
        )
    return configure_spark_with_delta_pip(builder).getOrCreate()
