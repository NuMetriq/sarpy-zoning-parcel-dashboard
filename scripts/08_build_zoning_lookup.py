"""
Create a lookup table of zoning_id to descriptive zoning names.
"""
from __future__ import annotations

import logging

import geopandas as gpd

from opsdash.common import Paths, configure_logging

LOGGER = logging.getLogger(__name__)

CANDIDATE_LABEL_COLS = (
    "zoning_code",
    "zone",
    "zoning",
    "district",
    "name",
    "dist",
    "descr",
    "description",
)


def main() -> int:
    configure_logging()
    paths = Paths()

    zoning_path = paths.processed_dir / "zoning.parquet"
    out_path = paths.processed_dir / "zoning_lookup.csv"

    if not zoning_path.exists():
        raise FileNotFoundError(f"Missing {zoning_path}. Run scripts/02_build_processed.py first.")

    z = gpd.read_parquet(zoning_path)
    if "zoning_id" not in z.columns:
        raise ValueError("Expected zoning_id in zoning.parquet")

    label_col = next((c for c in CANDIDATE_LABEL_COLS if c in z.columns), None)

    z = z.copy()
    z["zoning_name"] = z[label_col].astype(str) if label_col else z["zoning_id"].astype(str)

    out = z[["zoning_id", "zoning_name"]].drop_duplicates("zoning_id")
    out.to_csv(out_path, index=False)

    LOGGER.info("Wrote: %s", out_path)
    LOGGER.info("Label source: %s", label_col or "zoning_id")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())