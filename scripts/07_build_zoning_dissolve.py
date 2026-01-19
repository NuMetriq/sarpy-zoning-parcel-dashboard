from pathlib import Path
import geopandas as gpd
import pandas as pd
import shapely

PROCESSED_DIR = Path("data/processed")
IN_PATH = PROCESSED_DIR / "zoning.parquet"
OUT_PATH = PROCESSED_DIR / "zoning_dissolved.parquet"

# Use a local projected CRS for better geometric ops (Nebraska: UTM 14N)
WORK_CRS = 26914


def _repair_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Attempt to repair invalid geometries in a robust way:
      1) shapely.make_valid (best, if available)
      2) buffer(0) fallback
    """
    gdf = gdf.copy()

    # Drop empty geometries early
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()

    # Try shapely.make_valid if available (Shapely 2.x)
    if hasattr(shapely, "make_valid"):
        gdf["geometry"] = gdf["geometry"].apply(lambda geom: shapely.make_valid(geom) if geom is not None else geom)

    # buffer(0) often fixes self-intersections; safe fallback
    try:
        gdf["geometry"] = gdf["geometry"].buffer(0)
    except Exception:
        # If buffer fails for some rows, apply row-wise
        def _buf0(geom):
            try:
                return geom.buffer(0)
            except Exception:
                return geom
        gdf["geometry"] = gdf["geometry"].apply(_buf0)

    return gdf


def main():
    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing {IN_PATH}. Run scripts/02_build_processed.py first.")

    zoning = gpd.read_parquet(IN_PATH)

    # Ensure we start in WGS84
    zoning = zoning.set_crs(4326) if zoning.crs is None else zoning.to_crs(4326)

    if "zoning_code" not in zoning.columns:
        raise ValueError("Expected zoning_code in zoning.parquet (from ZONECLASS).")

    # Stable dissolve key (human-facing)
    zoning = zoning.copy()
    zoning["zoning_label"] = zoning["zoning_code"].astype(str)

    # Keep only what we need
    keep = ["zoning_label", "geometry"]
    if "zoning_desc" in zoning.columns:
        keep.append("zoning_desc")
    zoning = zoning[keep].copy()

    # Move to projected CRS for geometry operations (more stable)
    zoning_work = zoning.to_crs(WORK_CRS)

    # Repair geometries BEFORE dissolve
    zoning_work["was_valid"] = zoning_work.geometry.is_valid
    zoning_work = _repair_geometry(zoning_work)
    zoning_work["is_valid_after_repair"] = zoning_work.geometry.is_valid

    # Build a description lookup BEFORE dissolve (avoid trying to "aggregate" strings)
    desc_lookup = None
    if "zoning_desc" in zoning_work.columns:
        desc_lookup = (
            zoning_work[["zoning_label", "zoning_desc"]]
            .dropna()
            .drop_duplicates("zoning_label")
        )

    # Dissolve geometries only
    dissolved = zoning_work[["zoning_label", "geometry"]].dissolve(by="zoning_label", as_index=False)

    # Back to WGS84 for mapping
    dissolved = dissolved.to_crs(4326)

    # Reattach descriptions
    if desc_lookup is not None:
        dissolved = dissolved.merge(desc_lookup, on="zoning_label", how="left")

    # Write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    dissolved.to_parquet(OUT_PATH, index=False)

    # Report
    total = len(zoning_work)
    invalid_before = int((~zoning_work["was_valid"]).sum())
    invalid_after = int((~zoning_work["is_valid_after_repair"]).sum())

    print(f"Wrote: {OUT_PATH}")
    print(f"Rows: {len(dissolved):,} (unique zoning codes)")
    print(f"Invalid geometries before repair: {invalid_before:,} / {total:,}")
    print(f"Invalid geometries after repair:  {invalid_after:,} / {total:,}")
    if "zoning_desc" in dissolved.columns:
        print(dissolved[["zoning_label", "zoning_desc"]].head(10))


if __name__ == "__main__":
    main()