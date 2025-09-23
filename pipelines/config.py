from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    env: str = "dev"
    region: str = "us-central1"
    project_id: str | None = None
    bronze_bucket: str | None = None
    silver_bucket: str | None = None
    gold_bucket: str | None = None
    checkpoints_bucket: str | None = None
    local_lake_root: str = "./lake"
    iceberg_exports_dir: str | None = Field(
        default=None,
        validation_alias=AliasChoices("iceberg_exports_dir", "delta_exports_dir"),
    )
    iceberg_catalog: str = "skyhealth"
    iceberg_warehouse_uri: str | None = "medallion"
    use_dataproc: bool = False
    dataproc_region: str = "us-central1"
    dataproc_temp_bucket: str | None = None
    dataproc_service_account: str | None = None
    dataproc_subnet: str | None = None
    spark_executor_cores: str = "2"
    spark_executor_memory: str = "4g"
    bigquery_project: str | None = None
    bigquery_dataset: str = "skyhealth_dev_gold"
    duckdb_path: str = "./lake/dev.duckdb"
    ge_data_docs_path: str = "./great_expectations/uncommitted/data_docs/local_site"
    dagster_home: str = "./.dagster"
    locations_file: str = "data/locations.csv"
    log_level: str = "INFO"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "skyhealth"
    clickhouse_password: str | None = "skyhealth"
    clickhouse_secure: bool = False
    clickhouse_verify: bool = True
    clickhouse_database: str = "skyhealth"
    clickhouse_table: str = "climate_daily_summary"

    class Config:
        env_file = ".env"
        populate_by_name = True

    def bucket_uri(self, layer: str) -> str:
        bucket_attr = f"{layer}_bucket"
        bucket = getattr(self, bucket_attr, None)
        if bucket:
            return bucket
        root = Path(self.local_lake_root).resolve()
        return root.joinpath(layer).as_uri()

    def iceberg_warehouse(self) -> str:
        if self.iceberg_warehouse_uri:
            return self.iceberg_warehouse_uri
        root = Path(self.local_lake_root).resolve()
        return root.joinpath("warehouse").as_uri()

    def iceberg_namespace(self, layer: str) -> str:
        env_slug = self.env.replace("-", "_")
        return f"{env_slug}_{layer}"

    def iceberg_table_name(self, layer: str, table: str) -> str:
        return f"{self.iceberg_namespace(layer)}.{table}"

    def iceberg_table_identifier(self, layer: str, table: str) -> str:
        return f"{self.iceberg_catalog}.{self.iceberg_table_name(layer, table)}"

    def checkpoints_uri(self) -> str:
        if self.checkpoints_bucket:
            return self.checkpoints_bucket
        return str(Path(self.local_lake_root).resolve().joinpath("checkpoints").as_uri())

    def export_path(self, table: str) -> str:
        if self.iceberg_exports_dir:
            return str(Path(self.iceberg_exports_dir) / table)
        return str(Path(self.local_lake_root) / "exports" / table)

    def clickhouse_table_identifier(self) -> str:
        return f"{self.clickhouse_database}.{self.clickhouse_table}"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()


def export_path(table: str) -> str:
    return settings.export_path(table)
