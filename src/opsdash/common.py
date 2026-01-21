from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import geopandas as gpd
import pandas as pd


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def latest_subdir(root: Path) -> Path:
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")
    subdirs = sorted([p for p in root.iterdir() if p.is_dir()])
    if not subdirs:
        raise FileNotFoundError(f"No subdirectories found under: {root}")
    return subdirs[-1]


def normalize_arcgis_field(name: str) -> str:
    last = name.split(".")[-1]
    return (
        last.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .lower()
    )


def uniquify(names: Sequence[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        if n not in seen:
            seen[n] = 1
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
    return out


def ensure_crs(gdf: gpd.GeoDataFrame, epsg: int) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(epsg)
    return gdf.to_crs(epsg)


def coerce_id_column(
    df: pd.DataFrame,
    candidates: Iterable[str],
    fallback: str | None = None,
) -> pd.Series:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return df[c].astype(str)
    if fallback and fallback in cols:
        return df[fallback].astype(str)
    return pd.Series(range(len(df)), index=df.index, dtype="int64").astype(str)


def repair_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    out = out[out.geometry.notna() & ~out.geometry.is_empty].copy()

    if hasattr(out.geometry, "make_valid"):
        try:
            out["geometry"] = out.geometry.make_valid()
        except Exception:
            logging.getLogger(__name__).exception("make_valid failed; falling back to buffer(0).")

    try:
        out["geometry"] = out.geometry.buffer(0)
    except Exception:
        logging.getLogger(__name__).exception("buffer(0) failed; applying row-wise fallback.")
        out["geometry"] = out.geometry.apply(lambda geom: geom.buffer(0) if geom is not None else geom)

    return out


@dataclass(frozen=True)
class Paths:
    raw_root: Path = Path("data/raw/sarpy_gis")
    processed_dir: Path = Path("data/processed")