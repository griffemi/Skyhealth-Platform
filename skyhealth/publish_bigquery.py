from __future__ import annotations

import argparse
from datetime import date

import pandas as pd
from google.cloud import bigquery
from pyspark.sql import SparkSession, functions as F

from spark.jobs.utils.session import get_spark
from skyhealth.config import settings

GOLD_TABLE = settings.delta_table_uri("gold", "climate_daily_summary")
BQ_TABLE_NAME = "climate_daily_summary"


def _to_records(pdf: pd.DataFrame) -> list[dict]:
    pdf = pdf.copy()
    pdf["observation_date"] = pd.to_datetime(pdf["observation_date"]).dt.strftime("%Y-%m-%d")
    pdf["ingest_date"] = pd.to_datetime(pdf["ingest_date"]).dt.strftime("%Y-%m-%d")
    return pdf.to_dict(orient="records")


def publish_gold_partition(spark: SparkSession, target_date: date) -> None:
    project = settings.bigquery_project or settings.project_id
    if not project:
        raise ValueError("Set BIGQUERY project in settings before publishing.")

    dataset = settings.bigquery_dataset
    destination_table = f"{project}.{dataset}.{BQ_TABLE_NAME}"

    gold = spark.read.format("delta").load(GOLD_TABLE).where(F.col("observation_date") == F.lit(target_date))
    if not gold.head(1):
        return

    pdf = gold.toPandas()
    if pdf.empty:
        return

    records = _to_records(pdf)

    client = bigquery.Client(project=project)
    client.create_dataset(bigquery.Dataset(f"{project}.{dataset}"), exists_ok=True)

    schema = [
        bigquery.SchemaField("region_id", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("observation_date", "DATE"),
        bigquery.SchemaField("ingest_date", "DATE"),
        bigquery.SchemaField("avg_temp_c", "FLOAT"),
        bigquery.SchemaField("total_prcp_mm", "FLOAT"),
        bigquery.SchemaField("had_precip", "STRING"),
        bigquery.SchemaField("avg_hdd18", "FLOAT"),
        bigquery.SchemaField("avg_cdd18", "FLOAT"),
    ]

    partition_suffix = target_date.strftime("%Y%m%d")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(field="observation_date"),
        clustering_fields=["region_id"],
        schema=schema,
    )
    load_job = client.load_table_from_json(
        records,
        f"{destination_table}${partition_suffix}",
        job_config=job_config,
    )
    load_job.result()



def main() -> None:
    parser = argparse.ArgumentParser(description="Publish gold data to BigQuery")
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()
    target_date = date.fromisoformat(args.date)
    spark = get_spark("publish_gold", warehouse_uri=settings.bucket_uri("gold"))
    try:
        publish_gold_partition(spark, target_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
