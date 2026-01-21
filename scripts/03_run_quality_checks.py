"""
Run data quality checks on the processed Sarpy tax parcels dataset.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import geopandas as gpd

from opsdash.common import Paths, configure_logging

LOGGER = logging.getLogger(__name__)


def build_report(gdf: gpd.GeoDataFrame) -> dict[str, Any]:
    n = len(gdf)
    bbox = gdf.total_bounds.tolist() if n else [None, None, None, None]

    if "parcel_id" not in gdf.columns:
        raise ValueError("Expected 'parcel_id' column in parcels.parquet")

    missing_parcel_id = int(gdf["parcel_id"].isna().sum())
    dup_parcel_id = int(gdf["parcel_id"].duplicated().sum())

    geom_missing = int(gdf.geometry.isna().sum())
    geom_valid = int(gdf.geometry.is_valid.sum()) if n else 0
    geom_invalid = int(n - geom_valid - geom_missing)
    valid_rate = (geom_valid / n) if n else None

    return {
        "dataset": "sarpy_tax_parcels",
        "rows": n,
        "crs": str(gdf.crs),
        "bbox_wgs84": bbox,
        "parcel_id_missing": missing_parcel_id,
        "parcel_id_duplicates": dup_parcel_id,
        "geometry_missing": geom_missing,
        "geometry_valid": geom_valid,
        "geometry_invalid": geom_invalid,
        "geometry_valid_rate": valid_rate,
    }


def write_markdown(report: dict[str, Any], out_path: Path) -> None:
    n = report["rows"]
    lines = [
        "# Data Quality Report: Sarpy Tax Parcels",
        "",
        f"- Rows: **{n:,}**",
        f"- CRS: **{report['crs']}**",
        f"- BBox (WGS84): **{report['bbox_wgs84']}**",
        f"- parcel_id missing: **{report['parcel_id_missing']:,}**",
        f"- parcel_id duplicates: **{report['parcel_id_duplicates']:,}**",
        f"- geometry missing: **{report['geometry_missing']:,}**",
        f"- geometry valid: **{report['geometry_valid']:,}**",
        f"- geometry invalid: **{report['geometry_invalid']:,}**",
    ]
    rate = report.get("geometry_valid_rate")
    if rate is not None:
        lines.append(f"- geometry valid rate: **{rate:.4f}**")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    configure_logging()
    paths = Paths()

    parcels_path = paths.processed_dir / "parcels.parquet"
    if not parcels_path.exists():
        raise FileNotFoundError(f"Missing {parcels_path}. Run scripts/02_build_processed.py first.")

    gdf = gpd.read_parquet(parcels_path)
    report = build_report(gdf)

    out_json = paths.processed_dir / "data_quality_report_parcels.json"
    out_md = paths.processed_dir / "data_quality_report_parcels.md"

    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_markdown(report, out_md)

    LOGGER.info("Wrote: %s", out_json)
    LOGGER.info("Wrote: %s", out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())