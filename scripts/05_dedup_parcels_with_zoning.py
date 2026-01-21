"""
Checks for deduplication of parcels with zoning data.
"""
from __future__ import annotations

import logging

import geopandas as gpd

from opsdash.common import Paths, configure_logging, ensure_crs

LOGGER = logging.getLogger(__name__)

TARGET_CRS_EPSG = 4326
AREA_CRS_EPSG = 26914  # UTM 14N


def main() -> int:
    configure_logging()
    paths = Paths()

    joined_path = paths.processed_dir / "parcels_with_zoning.parquet"
    zoning_path = paths.processed_dir / "zoning.parquet"
    out_path = paths.processed_dir / "parcels_with_zoning_1to1.parquet"

    if not joined_path.exists():
        raise FileNotFoundError(f"Missing {joined_path}. Run scripts/04_build_spatial_joins.py first.")
    if not zoning_path.exists():
        raise FileNotFoundError(f"Missing {zoning_path}. Run scripts/02_build_processed.py first.")

    joined = ensure_crs(gpd.read_parquet(joined_path), TARGET_CRS_EPSG)
    zoning = ensure_crs(gpd.read_parquet(zoning_path), TARGET_CRS_EPSG)

    for col in ("parcel_id", "zoning_code"):
        if col not in joined.columns:
            raise ValueError(f"Expected {col} in {joined_path.name}")
    if "zoning_code" not in zoning.columns:
        raise ValueError("Expected zoning_code in zoning.parquet")

    base = joined.drop_duplicates("parcel_id").copy()
    counts = joined.groupby("parcel_id")["zoning_code"].nunique(dropna=True)
    multi_parcels = counts[counts > 1].index

    LOGGER.info("Parcels total: %s", f"{base['parcel_id'].nunique():,}")
    LOGGER.info("Parcels with multiple zoning matches: %s", f"{len(multi_parcels):,}")

    if len(multi_parcels) == 0:
        base.to_parquet(out_path, index=False)
        LOGGER.info("Wrote: %s (no overlaps found)", out_path)
        return 0

    cand = (
        joined.loc[joined["parcel_id"].isin(multi_parcels), ["parcel_id", "zoning_code"]]
        .drop_duplicates()
        .copy()
    )

    parcels_geom = base[["parcel_id", "geometry"]].copy()
    zoning_geom = zoning[["zoning_code", "geometry"]].drop_duplicates("zoning_code").copy()

    cand = cand.merge(parcels_geom, on="parcel_id", how="left")
    cand = gpd.GeoDataFrame(cand, geometry="geometry", crs=joined.crs)

    cand = cand.merge(
        zoning_geom.rename(columns={"geometry": "zoning_geom"}),
        on="zoning_code",
        how="left",
    )

    cand_area = cand.to_crs(AREA_CRS_EPSG)
    zoning_geom_series = gpd.GeoSeries(cand_area["zoning_geom"], index=cand_area.index).set_crs(
        AREA_CRS_EPSG, allow_override=True
    )
    cand_area["overlap_area_m2"] = cand_area.geometry.intersection(zoning_geom_series).area

    best = (
        cand_area.sort_values(["parcel_id", "overlap_area_m2"], ascending=[True, False])
        .drop_duplicates("parcel_id")[["parcel_id", "zoning_code"]]
        .copy()
    )

    base = base.drop(columns=[c for c in ("zoning_code", "zoning_desc") if c in base.columns], errors="ignore")
    base = base.merge(best, on="parcel_id", how="left")

    if "zoning_desc" in zoning.columns:
        desc = zoning[["zoning_code", "zoning_desc"]].drop_duplicates("zoning_code")
        base = base.merge(desc, on="zoning_code", how="left")

    base.to_parquet(out_path, index=False)

    matched = int(base["zoning_code"].notna().sum())
    total = len(base)
    LOGGER.info("Wrote: %s", out_path)
    LOGGER.info("Matched zoning_code: %s/%s (%.2f%%)", f"{matched:,}", f"{total:,}", (matched / total * 100) if total else 0.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())