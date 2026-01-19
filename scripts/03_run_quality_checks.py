from pathlib import Path
import json
import geopandas as gpd

PROCESSED = Path("data/processed/parcels.parquet")
OUT_JSON = Path("data/processed/data_quality_report_parcels.json")
OUT_MD = Path("data/processed/data_quality_report_parcels.md")


def main():
    if not PROCESSED.exists():
        raise FileNotFoundError("Missing data/processed/parcels.parquet. Run scripts/02_build_processed.py first.")

    gdf = gpd.read_parquet(PROCESSED)

    n = len(gdf)
    bbox = gdf.total_bounds.tolist()  # [minx, miny, maxx, maxy]
    crs = str(gdf.crs)

    missing_parcel_id = int(gdf["parcel_id"].isna().sum())
    dup_parcel_id = int(gdf["parcel_id"].duplicated().sum())

    geom_missing = int(gdf.geometry.isna().sum())
    geom_valid = int(gdf.geometry.is_valid.sum())
    geom_invalid = int(n - geom_valid - geom_missing)

    report = {
        "dataset": "sarpy_tax_parcels",
        "rows": n,
        "crs": crs,
        "bbox_wgs84": bbox,
        "parcel_id_missing": missing_parcel_id,
        "parcel_id_duplicates": dup_parcel_id,
        "geometry_missing": geom_missing,
        "geometry_valid": geom_valid,
        "geometry_invalid": geom_invalid,
        "geometry_valid_rate": (geom_valid / n) if n else None,
    }

    OUT_JSON.write_text(json.dumps(report, indent=2))

    md = []
    md.append("# Data Quality Report: Sarpy Tax Parcels\n")
    md.append(f"- Rows: **{n:,}**")
    md.append(f"- CRS: **{crs}**")
    md.append(f"- BBox (WGS84): **{bbox}**")
    md.append(f"- parcel_id missing: **{missing_parcel_id:,}**")
    md.append(f"- parcel_id duplicates: **{dup_parcel_id:,}**")
    md.append(f"- geometry missing: **{geom_missing:,}**")
    md.append(f"- geometry valid: **{geom_valid:,}**")
    md.append(f"- geometry invalid: **{geom_invalid:,}**")
    md.append(f"- geometry valid rate: **{report['geometry_valid_rate']:.4f}**")

    OUT_MD.write_text("\n".join(md) + "\n")

    print(f"Wrote: {OUT_JSON}")
    print(f"Wrote: {OUT_MD}")
    print("OK")


if __name__ == "__main__":
    main()