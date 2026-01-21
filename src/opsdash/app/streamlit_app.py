from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import geopandas as gpd
import pandas as pd
import pydeck as pdk
import streamlit as st

# Reuse shared utilities to avoid repetition across scripts + app
from opsdash.common import Paths, configure_logging, ensure_crs, repair_geometry

LOGGER = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
WGS84_EPSG = 4326
WORK_CRS_EPSG = 26914  # UTM 14N (good for Sarpy County geometry ops)

PATHS = Paths()
PARCELS_PATH = PATHS.processed_dir / "parcels_with_zoning_1to1.parquet"
ZONING_RAW_PATH = PATHS.processed_dir / "zoning.parquet"  # raw polygons (has jurisdiction)


# -------------------------------------------------------------------
# Data loading (cached)
# -------------------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_gdf_parquet(path: Path, mtime: float, epsg: int = WGS84_EPSG) -> gpd.GeoDataFrame:
    """
    Cached reader for GeoParquet. `mtime` is part of the cache key.
    """
    gdf = gpd.read_parquet(path)
    return ensure_crs(gdf, epsg)


def must_exist(path: Path, build_hint: str) -> None:
    if not path.exists():
        st.error(f"Missing {path}. {build_hint}")
        st.stop()


# -------------------------------------------------------------------
# Domain helpers
# -------------------------------------------------------------------
def view_state_from_bounds(gdf: gpd.GeoDataFrame) -> pdk.ViewState:
    minx, miny, maxx, maxy = gdf.total_bounds
    return pdk.ViewState(
        latitude=(miny + maxy) / 2,
        longitude=(minx + maxx) / 2,
        zoom=9.5,
        pitch=0,
    )


def alpha_from_ratio(x: int, max_x: int) -> int:
    """
    Return a semi-transparent alpha channel based on count intensity.
    """
    if max_x <= 0:
        max_x = 1
    a = 60 + int(175 * (x / max_x))
    return max(60, min(235, a))


def add_fill_color(map_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = map_gdf.copy()
    out["parcel_count"] = pd.to_numeric(out.get("parcel_count", 0), errors="coerce").fillna(0).astype(int)

    if out.empty:
        out["fill_color"] = []
        return out

    max_count = int(out["parcel_count"].max() or 1)
    out["fill_color"] = out["parcel_count"].apply(lambda x: [30, 120, 200, alpha_from_ratio(int(x), max_count)])
    return out


def parse_jurisdiction_labels() -> dict[int, str]:
    """
    Parse JURISDICTION_LABELS from env.
    Format: 10:Bellevue,20:Papillion,...
    """
    raw = os.getenv("JURISDICTION_LABELS", "")
    mapping: dict[int, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        try:
            mapping[int(k)] = v.strip()
        except ValueError:
            continue
    return mapping


def dissolve_zoning_by_code(zoning_filtered: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Dissolve zoning polygons by zoning_code within the filtered set.
    Uses shared repair_geometry() to avoid TopologyExceptions.
    """
    z = zoning_filtered.copy()

    if "zoning_code" not in z.columns:
        raise ValueError("Expected 'zoning_code' in zoning.parquet (produced by scripts/02_build_processed.py).")

    z["zoning_label"] = z["zoning_code"].astype(str)

    keep = ["zoning_label", "geometry"]
    if "zoning_desc" in z.columns:
        keep.append("zoning_desc")
    z = z[keep].copy()

    z = ensure_crs(z, WGS84_EPSG)

    # Keep one description per label (before dissolve)
    desc_lookup = None
    if "zoning_desc" in z.columns:
        desc_lookup = z[["zoning_label", "zoning_desc"]].dropna().drop_duplicates("zoning_label")

    # Robust union in projected CRS
    z_work = z.to_crs(WORK_CRS_EPSG)
    z_work = repair_geometry(z_work)

    dissolved = z_work[["zoning_label", "geometry"]].dissolve(by="zoning_label", as_index=False)
    dissolved = dissolved.to_crs(WGS84_EPSG)

    if desc_lookup is not None:
        dissolved = dissolved.merge(desc_lookup, on="zoning_label", how="left")

    return dissolved


def compute_rollups(parcels_filtered: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Roll up unique parcel counts by zoning_code.
    """
    required = {"parcel_id", "zoning_code"}
    missing = required - set(parcels_filtered.columns)
    if missing:
        raise ValueError(f"Parcels missing required columns: {sorted(missing)}")

    rollups = (
        parcels_filtered.dropna(subset=["zoning_code"])
        .groupby("zoning_code")["parcel_id"]
        .nunique()
        .reset_index()
        .rename(columns={"zoning_code": "zoning_label", "parcel_id": "parcel_count"})
    )
    rollups["zoning_label"] = rollups["zoning_label"].astype(str)
    rollups["parcel_count"] = rollups["parcel_count"].astype(int)
    return rollups


def build_tooltip(has_desc: bool) -> dict[str, Any]:
    if has_desc:
        html = (
            "<b>Zoning:</b> {zoning_label}<br/>"
            "<b>Description:</b> {zoning_desc}<br/>"
            "<b>Parcels:</b> {parcel_count}"
        )
    else:
        html = "<b>Zoning:</b> {zoning_label}<br/><b>Parcels:</b> {parcel_count}"

    return {
        "html": html,
        "style": {"backgroundColor": "white", "color": "black"},
    }


# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------
def main() -> None:
    configure_logging()

    st.set_page_config(page_title="Sarpy County Zoning Dashboard", layout="wide")
    st.title("Sarpy County Zoning Dashboard")
    st.caption("Parcels joined to zoning districts; choropleth shows parcel concentration by zoning code.")

    must_exist(
        PARCELS_PATH,
        "Build it with scripts/04_build_spatial_joins.py and scripts/05_dedup_parcels_with_zoning.py.",
    )
    must_exist(
        ZONING_RAW_PATH,
        "Build it with scripts/02_build_processed.py.",
    )

    parcels = load_gdf_parquet(PARCELS_PATH, PARCELS_PATH.stat().st_mtime, epsg=WGS84_EPSG)
    zoning_raw = load_gdf_parquet(ZONING_RAW_PATH, ZONING_RAW_PATH.stat().st_mtime, epsg=WGS84_EPSG)

    # Sidebar filters
    st.sidebar.header("Filters")

    selected_jurisdictions: Optional[list[int]] = None
    labels: dict[int, str] = {}

    if "jurisdiction" not in zoning_raw.columns:
        st.sidebar.warning("No 'jurisdiction' field found in zoning.parquet; jurisdiction filter disabled.")
    else:
        labels = parse_jurisdiction_labels()
        jvals = sorted([int(x) for x in zoning_raw["jurisdiction"].dropna().unique()])

        selected_jurisdictions = st.sidebar.multiselect(
            "Jurisdictions",
            options=jvals,
            default=jvals,
            format_func=lambda j: labels.get(j, f"Jurisdiction {j}"),
            help="Filter zoning polygons by jurisdiction before dissolving.",
        )

        if selected_jurisdictions is not None and len(selected_jurisdictions) == 0:
            st.warning("No jurisdictions selected. Choose at least one to display the map.")
            st.stop()

    # Apply zoning filter
    zoning_f = zoning_raw
    if selected_jurisdictions is not None:
        zoning_f = zoning_f[zoning_f["jurisdiction"].isin(selected_jurisdictions)].copy()

    # Dissolve filtered zoning (map layer)
    try:
        zoning_diss = dissolve_zoning_by_code(zoning_f)
    except Exception as exc:
        LOGGER.exception("Failed to dissolve zoning polygons")
        st.error(f"Failed to dissolve zoning polygons: {exc}")
        st.stop()

    # Filter parcels to codes present in filtered zoning set
    if "zoning_code" not in parcels.columns:
        st.error("Expected 'zoning_code' in parcels_with_zoning_1to1.parquet")
        st.stop()

    allowed_codes = set(zoning_f["zoning_code"].dropna().astype(str).unique())
    parcels_f = parcels[parcels["zoning_code"].astype(str).isin(allowed_codes)].copy()

    # Rollups + merge into dissolved polygons
    rollups = compute_rollups(parcels_f)
    zoning_diss["zoning_label"] = zoning_diss["zoning_label"].astype(str)

    map_gdf = zoning_diss.merge(rollups, on="zoning_label", how="left")
    map_gdf["parcel_count"] = map_gdf["parcel_count"].fillna(0).astype(int)

    # KPIs
    total_parcels = int(parcels_f["parcel_id"].nunique()) if "parcel_id" in parcels_f.columns else len(parcels_f)
    matched_parcels = int(parcels_f["zoning_code"].notna().sum()) if "zoning_code" in parcels_f.columns else 0
    unique_zones = int(rollups["zoning_label"].nunique())

    k1, k2, k3 = st.columns(3)
    k1.metric("Parcels (filtered)", f"{total_parcels:,}")
    k2.metric(
        "Parcels w/ Zoning",
        f"{matched_parcels:,}",
        f"{(matched_parcels / total_parcels):.2%}" if total_parcels else "0%",
    )
    k3.metric("Zoning Codes (filtered)", f"{unique_zones:,}")

    st.divider()

    left, right = st.columns([2, 1], gap="large")

    with right:
        st.subheader("Top Zoning Codes")
        cols = ["zoning_label", "parcel_count"]
        if "zoning_desc" in map_gdf.columns:
            cols = ["zoning_label", "zoning_desc", "parcel_count"]

        st.dataframe(
            map_gdf.drop(columns="geometry", errors="ignore")[cols]
            .sort_values("parcel_count", ascending=False)
            .head(25)
            .reset_index(drop=True),
            use_container_width=True,
            height=700,
        )

    with left:
        st.subheader("Choropleth: Parcel Count by Zoning")

        map_gdf = add_fill_color(map_gdf)
        geojson = json.loads(map_gdf.to_json())

        layer = pdk.Layer(
            "GeoJsonLayer",
            data=geojson,
            pickable=True,
            stroked=True,
            filled=True,
            extruded=False,
            wireframe=False,
            opacity=0.9,
            get_fill_color="properties.fill_color",
            get_line_color=[0, 0, 0, 140],
            get_line_width=50,
        )

        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state_from_bounds(map_gdf),
            tooltip=build_tooltip(has_desc=("zoning_desc" in map_gdf.columns)),
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        )

        st.pydeck_chart(deck, use_container_width=True)

    with st.expander("Debug"):
        st.write("Selected jurisdictions:", selected_jurisdictions)
        st.write("Zoning polygons (filtered):", len(zoning_f))
        st.write("Dissolved zoning codes:", len(zoning_diss))
        st.write("Max parcel_count:", int(map_gdf["parcel_count"].max() or 0))
        st.write(map_gdf[["zoning_label", "parcel_count"]].sort_values("parcel_count", ascending=False).head(10))
        if selected_jurisdictions is not None:
            st.write(
                "Jurisdiction labels:",
                {j: labels.get(j, f"Jurisdiction {j}") for j in selected_jurisdictions},
            )

    st.divider()
    st.caption(
        "Notes: Counts are computed from parcels_with_zoning_1to1.parquet and filtered to the selected jurisdiction(s). "
        "Zoning polygons are dissolved within the filter to keep the map readable."
    )


if __name__ == "__main__":
    main()