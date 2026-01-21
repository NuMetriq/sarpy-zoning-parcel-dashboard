"""
Build spatial joins between parcels and zoning datasets.
"""
from __future__ import annotations

import logging

import geopandas as gpd

from opsdash.common import Paths, configure_logging, ensure_crs

LOGGER = logging.getLogger(__name__)
TARGET_CRS_EPSG = 4326


def main() -> int:
    configure_logging()
    paths = Paths()

    parcels_path = paths.processed_dir / "parcels.parquet"
    zoning_path = paths.processed_dir / "zoning.parquet"
    out_path = paths.processed_dir / "parcels_with_zoning.parquet"

    if not parcels_path.exists():
        raise FileNotFoundError(f"Missing {parcels_path}. Run scripts/02_build_processed.py first.")
    if not zoning_path.exists():
        raise FileNotFoundError(f"Missing {zoning_path}. Run scripts/02_build_processed.py first.")

    parcels = ensure_crs(gpd.read_parquet(parcels_path), TARGET_CRS_EPSG)
    zoning = ensure_crs(gpd.read_parquet(zoning_path), TARGET_CRS_EPSG)

    required_parcels = {"parcel_id"}
    required_zoning = {"zoning_id", "zoning_code"}
    if required_parcels - set(parcels.columns):
        raise ValueError(f"Missing required parcel columns: {sorted(required_parcels - set(parcels.columns))}")
    if required_zoning - set(zoning.columns):
        raise ValueError(f"Missing required zoning columns: {sorted(required_zoning - set(zoning.columns))}")

    zoning_cols = ["zoning_id", "zoning_code"]
    if "zoning_desc" in zoning.columns:
        zoning_cols.append("zoning_desc")
    zoning_cols.append("geometry")

    joined = gpd.sjoin(parcels, zoning[zoning_cols], how="left", predicate="intersects")
    joined = joined.drop(columns=["index_right"], errors="ignore")

    joined.to_parquet(out_path, index=False)

    matched = int(joined["zoning_code"].notna().sum())
    total = len(joined)
    LOGGER.info("Wrote: %s", out_path)
    LOGGER.info("Join coverage: %s/%s (%.2f%%)", f"{matched:,}", f"{total:,}", (matched / total * 100) if total else 0.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())