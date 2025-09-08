from __future__ import annotations

from pathlib import Path
from pydantic import BaseSettings


class Settings(BaseSettings):
    warehouse_uri: str = "./lake/warehouse"
    checkpoint_dir: str = "./lake/checkpoints"
    parquet_export: str | None = None
    dbt_duckdb: str = "./lake/dev.duckdb"
    bq_project: str | None = None
    bq_dataset: str = "skyfeed_gold"
    spark_master_url: str = "spark://spark-master:7077"
    spark_master_rest_url: str = "http://spark-master:6066"
    spark_master_ui_url: str = "http://spark-master:8080"
    worker_project: str | None = None
    worker_zone: str = "us-central1-a"
    worker_mig_name: str | None = None
    worker_prescale: int = 3
    worker_postscale: int = 0
    worker_ready_target: int | None = None
    worker_register_timeout_sec: int = 600
    worker_register_poll_sec: int = 5
    spark_executor_cores: str = "2"
    spark_executor_memory: str = "4g"
    spark_task_max_failures: str = "4"
    spark_dra_enable: int = 1
    spark_shuffle_tracking_enable: int = 1
    spark_exec_min: int = 0
    spark_exec_init: int = 1
    spark_exec_max: int = 2
    poll_interval_sec: int = 10
    job_max_duration_sec: int = 7200
    dry_run: int = 0
    master_project: str | None = None
    master_zone: str = "us-central1-a"
    master_instance_name: str = "spark-master"
    master_start_timeout_sec: int = 300
    master_stop_timeout_sec: int = 300
    keep_master_after: int = 0
    log_level: str = "INFO"
    dbt_target: str = "prod"
    mode: str = "silver"

    class Config:
        env_file = ".env"


settings = Settings()


def export_path(table: str) -> str:
    if settings.parquet_export:
        return settings.parquet_export
    return str(Path(settings.warehouse_uri) / "exports" / table)
