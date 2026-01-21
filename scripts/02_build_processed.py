"""
Build processed GIS data from raw ingested files.
"""
from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd

from opsdash.common import (
    Paths,
    coerce_id_column,
    configure_logging,
    ensure_crs,
    latest_subdir,
    normalize_arcgis_field,
    uniquify,
)

LOGGER = logging.getLogger(__name__)

TARGET_CRS_EPSG = 4326  # WGS84

FILES = {
    "parcels": "sarpy_tax_parcels.geojson",
    "zoning": "sarpy_zoning.geojson",
    "neighborhoods": "sarpy_neighborhoods.geojson",
}


def to_processed(gdf: gpd.GeoDataFrame, kind: str) -> gpd.GeoDataFrame:
    out = gdf.copy()

    out.columns = uniquify([normalize_arcgis_field(c) for c in out.columns])
    out = ensure_crs(out, TARGET_CRS_EPSG)

    out["geom_is_valid"] = out.geometry.is_valid

    if kind == "parcels":
        out["parcel_id"] = coerce_id_column(
            out,
            candidates=("parcel_id", "parid", "par_id", "pin", "parcelno", "parcel_no"),
            fallback="objectid",
        )

    elif kind == "zoning":
        oid_candidates = [c for c in out.columns if c == "objectid" or c.startswith("objectid_")]
        if not oid_candidates:
            raise ValueError("No objectid-like column found in zoning after normalization.")
        oid_col = oid_candidates[0]

        out["zoning_id"] = out[oid_col].astype(str)

        if "zoneclass" in out.columns:
            out["zoning_code"] = out["zoneclass"].astype(str)
        if "zonedesc" in out.columns:
            out["zoning_desc"] = out["zonedesc"].astype(str)
        if "jurisdiction" in out.columns:
            out["jurisdiction"] = out["jurisdiction"]

    elif kind == "neighborhoods":
        oid_candidates = [c for c in out.columns if c == "objectid" or c.startswith("objectid_")]
        if oid_candidates:
            out["neighborhood_id"] = out[oid_candidates[0]].astype(str)

        name_candidates = [
            c
            for c in out.columns
            if c in ("name", "neighborhood", "neighborhood_name", "nbrhd", "nbrhd_name")
        ]
        if name_candidates:
            out["neighborhood_name"] = out[name_candidates[0]].astype(str)

    else:
        raise ValueError(f"Unknown kind: {kind}")

    return out


def main() -> int:
    configure_logging()
    paths = Paths()
    paths.processed_dir.mkdir(parents=True, exist_ok=True)

    raw_dir = latest_subdir(paths.raw_root)
    LOGGER.info("Using raw dir: %s", raw_dir)

    for kind, filename in FILES.items():
        src = raw_dir / filename
        if not src.exists():
            LOGGER.warning("Skipping %s: not found (%s)", kind, src.name)
            continue

        LOGGER.info("Reading %s: %s", kind, src.name)
        gdf = gpd.read_file(src)

        processed = to_processed(gdf, kind)
        out_path = paths.processed_dir / f"{kind}.parquet"
        processed.to_parquet(out_path, index=False)

        LOGGER.info("Wrote %s (rows=%s, cols=%s)", out_path, f"{len(processed):,}", f"{len(processed.columns):,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())