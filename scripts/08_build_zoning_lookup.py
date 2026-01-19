from pathlib import Path
import pandas as pd
import geopandas as gpd

PROCESSED_DIR = Path("data/processed")
ZONING_PATH = PROCESSED_DIR / "zoning.parquet"
OUT_PATH = PROCESSED_DIR / "zoning_lookup.csv"

CANDIDATE_LABEL_COLS = ["zoning_code", "zone", "zoning", "district", "name", "dist", "descr", "description"]


def main():
    if not ZONING_PATH.exists():
        raise FileNotFoundError(f"Missing {ZONING_PATH}")

    z = gpd.read_parquet(ZONING_PATH)

    if "zoning_id" not in z.columns:
        raise ValueError("Expected zoning_id in zoning.parquet")

    # pick the best descriptive column available
    label_col = None
    for c in CANDIDATE_LABEL_COLS:
        if c in z.columns:
            label_col = c
            break

    if label_col is None:
        # fall back: at least provide zoning_id as label
        z["zoning_name"] = z["zoning_id"].astype(str)
        label_col = "zoning_name"
    else:
        z["zoning_name"] = z[label_col].astype(str)

    # one row per zoning_id
    out = (
        z[["zoning_id", "zoning_name"]]
        .drop_duplicates("zoning_id")
        .copy()
    )

    out.to_csv(OUT_PATH, index=False)

    print(f"Wrote: {OUT_PATH}")
    print("Label source column:", label_col)
    print(out.head(10))


if __name__ == "__main__":
    main()