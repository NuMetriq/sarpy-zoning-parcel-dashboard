from pathlib import Path
import geopandas as gpd
import pandas as pd

PROCESSED_DIR = Path("data/processed")
JOINED_PATH = PROCESSED_DIR / "parcels_with_zoning.parquet"
ZONING_PATH = PROCESSED_DIR / "zoning.parquet"
OUT_PATH = PROCESSED_DIR / "parcels_with_zoning_1to1.parquet"

# Nebraska UTM zone 14N for area calculations
AREA_CRS = 26914


def main():
    if not JOINED_PATH.exists():
        raise FileNotFoundError(f"Missing {JOINED_PATH}. Run scripts/04_build_spatial_joins.py first.")
    if not ZONING_PATH.exists():
        raise FileNotFoundError(f"Missing {ZONING_PATH}. Run scripts/02_build_processed.py first.")

    joined = gpd.read_parquet(JOINED_PATH)
    zoning = gpd.read_parquet(ZONING_PATH)

    # Ensure WGS84
    joined = joined.set_crs(4326) if joined.crs is None else joined.to_crs(4326)
    zoning = zoning.set_crs(4326) if zoning.crs is None else zoning.to_crs(4326)

    if "parcel_id" not in joined.columns:
        raise ValueError("Expected parcel_id in parcels_with_zoning.parquet")
    if "zoning_code" not in joined.columns:
        raise ValueError("Expected zoning_code in parcels_with_zoning.parquet. Ensure 04_build_spatial_joins.py keeps it.")
    if "zoning_code" not in zoning.columns:
        raise ValueError("Expected zoning_code in zoning.parquet")

    # Base: one row per parcel (keep parcel geometry + attributes)
    base = joined.drop_duplicates("parcel_id").copy()

    # Find parcels with multiple zoning matches
    counts = joined.groupby("parcel_id")["zoning_code"].nunique(dropna=True)
    multi = counts[counts > 1].index.tolist()

    print(f"Parcels total: {base['parcel_id'].nunique():,}")
    print(f"Parcels with multiple zoning matches: {len(multi):,}")

    if len(multi) == 0:
        # Already 1-to-1
        base.to_parquet(OUT_PATH, index=False)
        print(f"Wrote: {OUT_PATH} (no overlaps found)")
        return

    # Candidate pairs from the joined table (only multi parcels)
    cand = joined[joined["parcel_id"].isin(multi)][["parcel_id", "zoning_code"]].drop_duplicates()

    # Attach geometries: parcel geometry from base, zoning geometry from zoning table
    parcels_geom = base[["parcel_id", "geometry"]].copy()
    zoning_geom = zoning[["zoning_code", "geometry"]].drop_duplicates("zoning_code").copy()

    cand = cand.merge(parcels_geom, on="parcel_id", how="left")
    cand = gpd.GeoDataFrame(cand, geometry="geometry", crs=joined.crs)

    cand = cand.merge(
        zoning_geom.rename(columns={"geometry": "zoning_geom"}),
        on="zoning_code",
        how="left",
    )

    missing_z = cand["zoning_geom"].isna().sum()
    if missing_z:
        print(f"Warning: {missing_z} candidate rows missing zoning geometry (will get NaN overlap).")

    # Compute overlap area in projected CRS
    cand_area = cand.to_crs(AREA_CRS)

    # Convert zoning_geom into a proper GeoSeries without CRS conflicts
    zoning_geom_series = gpd.GeoSeries(cand_area["zoning_geom"], index=cand_area.index)
    zoning_geom_series = zoning_geom_series.set_crs(AREA_CRS, allow_override=True)

    cand_area["overlap_area_m2"] = cand_area.geometry.intersection(zoning_geom_series).area

    # Pick best zoning_code per parcel_id by max overlap
    best = (
        cand_area.sort_values(["parcel_id", "overlap_area_m2"], ascending=[True, False])
        .drop_duplicates("parcel_id")[["parcel_id", "zoning_code", "overlap_area_m2"]]
        .copy()
    )

    # Merge best zoning_code back into base (overwrite zoning_code for those multi parcels)
    base = base.drop(columns=[c for c in ["zoning_code", "zoning_desc"] if c in base.columns], errors="ignore")
    base = base.merge(best[["parcel_id", "zoning_code"]], on="parcel_id", how="left")

    # Bring zoning_desc back (optional, for dashboard)
    if "zoning_desc" in zoning.columns:
        desc = zoning[["zoning_code", "zoning_desc"]].drop_duplicates("zoning_code")
        base = base.merge(desc, on="zoning_code", how="left")

    # Coverage
    matched = int(base["zoning_code"].notna().sum())
    total = len(base)

    base.to_parquet(OUT_PATH, index=False)

    print(f"Wrote: {OUT_PATH}")
    print(f"1-to-1 parcels: {total:,}")
    print(f"Matched zoning_code: {matched:,}/{total:,} ({matched/total:.4%})")


if __name__ == "__main__":
    main()