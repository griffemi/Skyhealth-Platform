from __future__ import annotations

from pyspark.sql import functions as F

from skyfeed.config import export_path
from spark.jobs.utils.cli import base_parser
from spark.jobs.utils.log import configure
from spark.jobs.utils.session import get_spark
from spark.jobs.utils.schemas import SILVER_DAILY_SCHEMA


def _upsert(df, table: str, spark) -> None:
    df.createOrReplaceTempView("updates")
    spark.sql(
        f"""
        MERGE INTO local.{table} t
        USING updates u
        ON t.location_id = u.location_id AND t.d = u.d
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    configure()
    parser = base_parser("Silver climate features")
    args = parser.parse_args()
    spark = get_spark("silver_climate_daily_features", warehouse_uri=args.warehouse_uri)
    bronze = spark.read.table("local.climate_bronze_daily")
    df = (
        bronze
        .withColumn("hdd18", F.greatest(F.lit(0.0), F.lit(18.0) - F.col("tavg_c")))
        .withColumn("cdd18", F.greatest(F.lit(0.0), F.col("tavg_c") - F.lit(18.0)))
        .withColumn(
            "gdd10_30",
            F.greatest(
                F.lit(0.0),
                F.least(F.lit(30.0), F.greatest(F.col("tavg_c"), F.lit(10.0))) - F.lit(10.0),
            ),
        )
        .withColumn("prcp_flag", F.when(F.col("prcp_mm") > 0, "Y").otherwise("N"))
    )
    _upsert(df.select(SILVER_DAILY_SCHEMA.fieldNames()), "climate_silver_daily_features", spark)
    df.write.mode("overwrite").partitionBy("d").parquet(export_path("climate_silver_daily_features"))
    spark.stop()


if __name__ == "__main__":
    main()
