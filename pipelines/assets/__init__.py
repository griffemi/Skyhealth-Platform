"""Convenience imports for asset jobs exposed via dagster entrypoints."""

from importlib import import_module
from types import ModuleType


def _load(module_path: str) -> ModuleType:
    return import_module(module_path, package=__name__)


bronze_openmeteo = _load(".01_bronze.openmeteo")
silver_climate_daily_features = _load(".02_silver.climate_daily_features")
gold_climate_daily_summary = _load(".03_gold.climate_daily_summary")

__all__ = [
    "bronze_openmeteo",
    "silver_climate_daily_features",
    "gold_climate_daily_summary",
]
