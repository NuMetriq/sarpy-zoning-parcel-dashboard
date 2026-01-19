from pathlib import Path
import geopandas as gpd

PROCESSED_DIR = Path("data/processed")


def main():
    parcels_path = PROCESSED_DIR / "parcels.parquet"
    zoning_path = PROCESSED_DIR / "zoning.parquet"

    if not parcels_path.exists():
        raise FileNotFoundError(f"Missing {parcels_path}. Run scripts/02_build_processed.py first.")
    if not zoning_path.exists():
        raise FileNotFoundError(f"Missing {zoning_path}. Run scripts/02_build_processed.py first.")

    print("Reading processed parcels + zoning...")
    parcels = gpd.read_parquet(parcels_path)
    zoning = gpd.read_parquet(zoning_path)

    # Ensure WGS84
    parcels = parcels.set_crs(4326) if parcels.crs is None else parcels.to_crs(4326)
    zoning = zoning.set_crs(4326) if zoning.crs is None else zoning.to_crs(4326)

    # Require parcel_id
    if "parcel_id" not in parcels.columns:
        raise ValueError("Expected parcel_id in parcels.parquet")

    # Require zoning_id and zoning_code from your processed zoning
    if "zoning_id" not in zoning.columns:
        raise ValueError("Expected zoning_id in zoning.parquet")
    if "zoning_code" not in zoning.columns:
        raise ValueError("Expected zoning_code in zoning.parquet (ZONECLASS)")

    # Keep only the zoning fields we care about + geometry
    zoning_small = zoning[["zoning_id", "zoning_code"] + (["zoning_desc"] if "zoning_desc" in zoning.columns else []) + ["geometry"]].copy()

    print("Spatial join: parcels -> zoning (predicate=intersects)...")
    joined = gpd.sjoin(parcels, zoning_small, how="left", predicate="intersects")

    if "index_right" in joined.columns:
        joined = joined.drop(columns=["index_right"])

    out_path = PROCESSED_DIR / "parcels_with_zoning.parquet"
    joined.to_parquet(out_path, index=False)

    # Coverage
    matched = int(joined["zoning_code"].notna().sum())
    total = len(joined)
    print(f"Join coverage (zoning_code not null): {matched:,}/{total:,} ({matched/total:.4%})")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()