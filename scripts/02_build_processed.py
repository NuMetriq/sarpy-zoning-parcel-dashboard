from pathlib import Path
import geopandas as gpd
import pandas as pd

RAW_ROOT = Path("data/raw/sarpy_gis")
PROCESSED_DIR = Path("data/processed")

FILES = {
    "parcels": "sarpy_tax_parcels.geojson",
    "zoning": "sarpy_zoning.geojson",
    "neighborhoods": "sarpy_neighborhoods.geojson",
}


def latest_raw_dir() -> Path:
    dated = sorted([p for p in RAW_ROOT.iterdir() if p.is_dir()])
    if not dated:
        raise FileNotFoundError("No data/raw/sarpy_gis/<date>/ folders found. Run 01_ingest_all.py first.")
    return dated[-1]


def normalize_arcgis_field(name: str) -> str:
    # last token after dot, lower, simple cleanup
    last = name.split(".")[-1]
    return last.strip().replace(" ", "_").replace("-", "_").replace("/", "_").lower()


def uniquify(names: list[str]) -> list[str]:
    """
    Make duplicate column names unique by appending _2, _3, ...
    Example: ["objectid","zoneclass","objectid"] -> ["objectid","zoneclass","objectid_2"]
    """
    seen = {}
    out = []
    for n in names:
        if n not in seen:
            seen[n] = 1
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
    return out


def to_processed(gdf: gpd.GeoDataFrame, kind: str) -> gpd.GeoDataFrame:
    gdf = gdf.copy()

    # Normalize and uniquify column names
    normalized = [normalize_arcgis_field(c) for c in gdf.columns]
    gdf.columns = uniquify(normalized)

    # Ensure WGS84
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    else:
        gdf = gdf.to_crs(4326)

    # Geometry validity flag
    gdf["geom_is_valid"] = gdf.geometry.is_valid

    if kind == "parcels":
        candidates = [c for c in gdf.columns if c in ("parcel_id", "parid", "par_id", "pin", "parcelno", "parcel_no")]
        if candidates:
            gdf["parcel_id"] = gdf[candidates[0]].astype(str)
        elif "objectid" in gdf.columns:
            gdf["parcel_id"] = gdf["objectid"].astype(str)
        else:
            gdf["parcel_id"] = pd.Series(range(len(gdf))).astype(str)

    elif kind == "zoning":
        # Find the BEST objectid for zoning district:
        # Prefer plain "objectid" if present; otherwise take the first objectid* column
        oid_candidates = [c for c in gdf.columns if c == "objectid" or c.startswith("objectid_")]
        if not oid_candidates:
            raise ValueError("No objectid-like column found in zoning data after normalization.")
        oid_col = oid_candidates[0]
        gdf["zoning_id"] = gdf[oid_col].astype(str)

        # Human-friendly label fields from Planning/ZoningDynamic
        if "zoneclass" in gdf.columns:
            gdf["zoning_code"] = gdf["zoneclass"].astype(str)
        if "zonedesc" in gdf.columns:
            gdf["zoning_desc"] = gdf["zonedesc"].astype(str)
        if "jurisdiction" in gdf.columns:
            gdf["jurisdiction"] = gdf["jurisdiction"]

    elif kind == "neighborhoods":
        # Best-effort IDs/labels
        oid_candidates = [c for c in gdf.columns if c == "objectid" or c.startswith("objectid_")]
        if oid_candidates:
            gdf["neighborhood_id"] = gdf[oid_candidates[0]].astype(str)

        name_candidates = [c for c in gdf.columns if c in ("name", "neighborhood", "neighborhood_name", "nbrhd", "nbrhd_name")]
        if name_candidates:
            gdf["neighborhood_name"] = gdf[name_candidates[0]].astype(str)

    return gdf


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    raw_dir = latest_raw_dir()
    print(f"Using raw dir: {raw_dir}")

    for kind, filename in FILES.items():
        src = raw_dir / filename
        if not src.exists():
            print(f"Skipping {kind}: not found ({src.name})")
            continue

        print(f"Reading {kind}: {src.name}")
        gdf = gpd.read_file(src)
        gdf = to_processed(gdf, kind)

        out = PROCESSED_DIR / f"{kind}.parquet"
        gdf.to_parquet(out, index=False)
        print(f"Wrote {out}  (rows={len(gdf):,}, cols={len(gdf.columns):,})")

        # Quick zoning sanity check
        if kind == "zoning":
            cols = [c for c in ["zoning_id", "zoning_code", "zoning_desc", "jurisdiction"] if c in gdf.columns]
            print("Zoning key columns:", cols)
            if "zoning_code" in gdf.columns:
                print("Sample zoning codes/descs:")
                print(gdf[["zoning_code", "zoning_desc"]].drop_duplicates().head(10))


if __name__ == "__main__":
    main()