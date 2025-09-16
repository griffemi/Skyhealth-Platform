from __future__ import annotations

from pathlib import Path

import pandas as pd

from skyhealth.config import settings


class ValidationError(Exception):
    """Raised when data fails simple contract checks."""


def _write_snapshot(suite: str, partition_key: str, frame: pd.DataFrame) -> None:
    docs_dir = Path(settings.ge_data_docs_path).with_suffix("") / suite
    docs_dir.mkdir(parents=True, exist_ok=True)
    output = docs_dir / f"{partition_key}.html"
    output.write_text(frame.head().to_html(index=False), encoding="utf-8")


def _require_columns(frame: pd.DataFrame, columns: list[str], suite: str, partition_key: str) -> None:
    missing = [col for col in columns if frame[col].isna().any()]
    if missing:
        raise ValidationError(f"Missing values detected in columns {missing} for {suite}:{partition_key}")


def _require_range(frame: pd.DataFrame, column: str, min_value: float | None = None, max_value: float | None = None) -> None:
    series = frame[column]
    if min_value is not None and (series < min_value).any():
        raise ValidationError(f"Column {column} below minimum {min_value}")
    if max_value is not None and (series > max_value).any():
        raise ValidationError(f"Column {column} above maximum {max_value}")


def validate_silver(partition_key: str, dataframe: pd.DataFrame) -> None:
    _write_snapshot("silver_climate_daily_features", partition_key, dataframe)
    _require_columns(dataframe, ["location_id", "observation_date"], "silver", partition_key)
    if dataframe["tavg_c"].isna().all() and dataframe["prcp_mm"].isna().all():
        raise ValidationError("Silver partition is empty after transformations")
    _require_range(dataframe, "tavg_c", min_value=-90, max_value=60)
    _require_range(dataframe, "prcp_mm", min_value=0)


def validate_gold(partition_key: str, dataframe: pd.DataFrame) -> None:
    _write_snapshot("gold_climate_daily_summary", partition_key, dataframe)
    _require_columns(dataframe, ["region_id", "observation_date"], "gold", partition_key)
    _require_range(dataframe, "total_prcp_mm", min_value=0)
    if dataframe["avg_temp_c"].isna().all():
        raise ValidationError("Gold partition has no temperature data")
