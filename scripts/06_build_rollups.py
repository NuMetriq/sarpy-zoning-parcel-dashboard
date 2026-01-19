from pathlib import Path
import pandas as pd
import geopandas as gpd

PROCESSED_DIR = Path("data/processed")

def main():
    inp = PROCESSED_DIR / "parcels_with_zoning_1to1.parquet"
    if not inp.exists():
        raise FileNotFoundError(f"Missing {inp}")

    gdf = gpd.read_parquet(inp)

    if "zoning_id" not in gdf.columns:
        raise ValueError("Expected zoning_id in parcels_with_zoning_1to1.parquet")

    # roll up by zoning_id only
    grp = gdf.groupby("zoning_id", dropna=False)

    out = pd.DataFrame({
        "zoning_label": grp["zoning_id"].first(),        # keep column name expected by app
        "parcel_count": grp["parcel_id"].nunique(),
    }).reset_index(drop=True)

    out_path = PROCESSED_DIR / "zoning_rollups.csv"
    out.to_csv(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"Rows (zoning groups): {len(out):,}")
    print("Top 10 zoning_id by parcel_count:")
    print(out.sort_values("parcel_count", ascending=False).head(10))

if __name__ == "__main__":
    main()