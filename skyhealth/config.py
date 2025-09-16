from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):
    env: str = "dev"
    region: str = "us-central1"
    project_id: str | None = None
    bronze_bucket: str | None = None
    silver_bucket: str | None = None
    gold_bucket: str | None = None
    checkpoints_bucket: str | None = None
    local_lake_root: str = "./lake"
    delta_exports_dir: str | None = None
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

    class Config:
        env_file = ".env"

    def bucket_uri(self, layer: str) -> str:
        bucket_attr = f"{layer}_bucket"
        bucket = getattr(self, bucket_attr, None)
        if bucket:
            return bucket
        root = Path(self.local_lake_root).resolve()
        return root.joinpath(layer).as_uri()

    def delta_table_uri(self, layer: str, table: str) -> str:
        base = self.bucket_uri(layer)
        if base.startswith("file://"):
            return f"{base}/{table}"
        return f"{base}/{table}"

    def checkpoints_uri(self) -> str:
        if self.checkpoints_bucket:
            return self.checkpoints_bucket
        return str(Path(self.local_lake_root).resolve().joinpath("checkpoints").as_uri())

    def export_path(self, table: str) -> str:
        if self.delta_exports_dir:
            return str(Path(self.delta_exports_dir) / table)
        return str(Path(self.local_lake_root) / "exports" / table)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
