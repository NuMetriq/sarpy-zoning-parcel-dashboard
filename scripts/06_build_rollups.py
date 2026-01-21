"""
Build zoning rollups from deduplicated parcels with zoning.
"""
from __future__ import annotations

import logging

import geopandas as gpd
import pandas as pd

from opsdash.common import Paths, configure_logging

LOGGER = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    paths = Paths()

    in_path = paths.processed_dir / "parcels_with_zoning_1to1.parquet"
    out_path = paths.processed_dir / "zoning_rollups.csv"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing {in_path}. Run scripts/05_dedup_parcels_with_zoning.py first.")

    gdf = gpd.read_parquet(in_path)

    required = {"parcel_id", "zoning_id"}
    missing = required - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing required columns in {in_path.name}: {sorted(missing)}")

    grp = gdf.groupby("zoning_id", dropna=False)
    out = (
        pd.DataFrame(
            {
                "zoning_label": grp["zoning_id"].first(),
                "parcel_count": grp["parcel_id"].nunique(),
            }
        )
        .reset_index(drop=True)
    )

    out.to_csv(out_path, index=False)
    LOGGER.info("Wrote: %s", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())