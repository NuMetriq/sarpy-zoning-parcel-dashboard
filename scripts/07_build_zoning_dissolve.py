"""
Build a dissolved zoning dataset from the processed zoning data.
"""
from __future__ import annotations

import logging

import geopandas as gpd

from opsdash.common import Paths, configure_logging, ensure_crs, repair_geometry

LOGGER = logging.getLogger(__name__)

WGS84_EPSG = 4326
WORK_CRS_EPSG = 26914


def main() -> int:
    configure_logging()
    paths = Paths()

    in_path = paths.processed_dir / "zoning.parquet"
    out_path = paths.processed_dir / "zoning_dissolved.parquet"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing {in_path}. Run scripts/02_build_processed.py first.")

    zoning = gpd.read_parquet(in_path)
    zoning = ensure_crs(zoning, WGS84_EPSG)

    if "zoning_code" not in zoning.columns:
        raise ValueError("Expected zoning_code in zoning.parquet (from ZONECLASS).")

    zoning = zoning.copy()
    zoning["zoning_label"] = zoning["zoning_code"].astype(str)

    keep = ["zoning_label", "geometry"]
    if "zoning_desc" in zoning.columns:
        keep.append("zoning_desc")
    zoning = zoning[keep].copy()

    zoning_work = zoning.to_crs(WORK_CRS_EPSG)
    zoning_work = repair_geometry(zoning_work)

    desc_lookup = None
    if "zoning_desc" in zoning_work.columns:
        desc_lookup = zoning_work[["zoning_label", "zoning_desc"]].dropna().drop_duplicates("zoning_label")

    dissolved = zoning_work[["zoning_label", "geometry"]].dissolve(by="zoning_label", as_index=False)
    dissolved = dissolved.to_crs(WGS84_EPSG)

    if desc_lookup is not None:
        dissolved = dissolved.merge(desc_lookup, on="zoning_label", how="left")

    dissolved.to_parquet(out_path, index=False)
    LOGGER.info("Wrote: %s", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())