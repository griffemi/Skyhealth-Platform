from __future__ import annotations

import argparse
from datetime import date

import pandas as pd
from google.cloud import bigquery
from pyspark.sql import SparkSession, functions as F
from pyspark.errors.exceptions.base import AnalysisException

from pipelines.clickhouse_io import ClickHouseError, ClickHousePublisher
from pipelines.config import settings
from pipelines.utils.session import get_spark

GOLD_TABLE = settings.iceberg_table_identifier("gold", "climate_daily_summary")
BQ_TABLE_NAME = "climate_daily_summary"


class GoldPartitionPublisher:
    """Environment-aware publisher that routes to BigQuery in prod, ClickHouse otherwise."""

    def __init__(self, environment: str | None = None) -> None:
        self._environment = (environment or settings.env).lower()
        self._clickhouse = ClickHousePublisher()

    def publish(self, pdf: pd.DataFrame, target_date: date) -> None:
        if self._environment == "prod":
            self._publish_bigquery(pdf, target_date)
        else:
            self._publish_clickhouse(pdf, target_date)

    @staticmethod
    def _prepare_records(pdf: pd.DataFrame) -> list[dict]:
        frame = pdf.copy()
        frame["observation_date"] = pd.to_datetime(frame["observation_date"]).dt.strftime("%Y-%m-%d")
        frame["ingest_date"] = pd.to_datetime(frame["ingest_date"]).dt.strftime("%Y-%m-%d")
        return frame.to_dict(orient="records")

    def _publish_bigquery(self, pdf: pd.DataFrame, target_date: date) -> None:
        project = settings.bigquery_project or settings.project_id
        if not project:
            raise ValueError("Set BIGQUERY project in settings before publishing.")

        dataset = settings.bigquery_dataset
        destination_table = f"{project}.{dataset}.{BQ_TABLE_NAME}"

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
            self._prepare_records(pdf),
            f"{destination_table}${partition_suffix}",
            job_config=job_config,
        )
        load_job.result()

    def _publish_clickhouse(self, pdf: pd.DataFrame, target_date: date) -> None:
        try:
            self._clickhouse.publish(pdf, target_date)
        except ClickHouseError as exc:
            raise RuntimeError(
                "Failed to publish to ClickHouse. Ensure the ClickHouse server is running and configured."
            ) from exc


_PUBLISHER = GoldPartitionPublisher()


def publish_gold_partition(spark: SparkSession, target_date: date) -> None:
    try:
        gold = spark.read.table(GOLD_TABLE).where(F.col("observation_date") == F.lit(target_date))
    except AnalysisException as exc:
        message = getattr(exc, "desc", str(exc))
        if "TABLE_OR_VIEW_NOT_FOUND" in message:
            print("Gold table not found. Materialize Iceberg tables before publishing.")
            return
        raise
    if not gold.head(1):
        return

    pdf = gold.toPandas()
    if pdf.empty:
        return

    _PUBLISHER.publish(pdf, target_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish gold data to downstream stores")
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()
    target_date = date.fromisoformat(args.date)
    spark = get_spark("publish_gold")
    try:
        publish_gold_partition(spark, target_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
