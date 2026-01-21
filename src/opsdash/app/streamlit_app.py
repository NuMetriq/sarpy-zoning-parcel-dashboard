from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
import pydeck as pdk
import streamlit as st

from opsdash.common import Paths, configure_logging, ensure_crs, repair_geometry

LOGGER = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
WGS84_EPSG = 4326
WORK_CRS_EPSG = 26914  # UTM 14N (good for Sarpy County geometry ops)
M2_TO_ACRES = 0.0002471053814671653  # exact conversion

PATHS = Paths()
PARCELS_PATH = PATHS.processed_dir / "parcels_with_zoning_1to1.parquet"
ZONING_RAW_PATH = PATHS.processed_dir / "zoning.parquet"  # raw polygons (has jurisdiction)


# -------------------------------------------------------------------
# Data loading (cached)
# -------------------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_gdf_parquet(path: Path, mtime: float, epsg: int = WGS84_EPSG) -> gpd.GeoDataFrame:
    """Cached reader for GeoParquet. `mtime` is part of the cache key."""
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
    """Return a semi-transparent alpha channel based on count intensity."""
    if max_x <= 0:
        max_x = 1
    a = 60 + int(175 * (x / max_x))
    return max(60, min(235, a))


def add_fill_color(map_gdf: gpd.GeoDataFrame, *, metric_col: str) -> gpd.GeoDataFrame:
    """
    Color polygons by the selected metric (count, acres, or percent).
    Uses alpha intensity scaling.
    """
    out = map_gdf.copy()

    out[metric_col] = pd.to_numeric(out.get(metric_col, 0), errors="coerce").fillna(0)

    if out.empty:
        out["fill_color"] = []
        return out

    max_val = float(out[metric_col].max() or 0.0)
    if max_val <= 0:
        max_val = 1.0

    def to_alpha(val: float) -> int:
        a = 60 + int(175 * (float(val) / max_val))
        return max(60, min(235, a))

    out["fill_color"] = out[metric_col].apply(lambda v: [30, 120, 200, to_alpha(v)])
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

    desc_lookup = None
    if "zoning_desc" in z.columns:
        desc_lookup = z[["zoning_label", "zoning_desc"]].dropna().drop_duplicates("zoning_label")

    z_work = z.to_crs(WORK_CRS_EPSG)
    z_work = repair_geometry(z_work)

    dissolved = z_work[["zoning_label", "geometry"]].dissolve(by="zoning_label", as_index=False)
    dissolved = dissolved.to_crs(WGS84_EPSG)

    if desc_lookup is not None:
        dissolved = dissolved.merge(desc_lookup, on="zoning_label", how="left")

    return dissolved


def compute_rollups(parcels_filtered: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Roll up unique parcel counts + parcel area metrics by zoning_code.
    Parcel areas are computed in EPSG:26914 for accurate area calculations.
    """
    required = {"parcel_id", "zoning_code", "geometry"}
    missing = required - set(parcels_filtered.columns)
    if missing:
        raise ValueError(f"Parcels missing required columns: {sorted(missing)}")

    df = parcels_filtered.dropna(subset=["zoning_code"]).copy()

    df_area = df.to_crs(WORK_CRS_EPSG)
    df_area["parcel_area_m2"] = df_area.geometry.area

    grp = df_area.groupby(df_area["zoning_code"].astype(str), dropna=False)

    out = pd.DataFrame(
        {
            "zoning_label": grp["zoning_code"].first().astype(str),
            "parcel_count": grp["parcel_id"].nunique(),
            "total_parcel_area_acres": grp["parcel_area_m2"].sum() * M2_TO_ACRES,
            "median_parcel_area_acres": grp["parcel_area_m2"].median() * M2_TO_ACRES,
        }
    ).reset_index(drop=True)

    out["parcel_count"] = out["parcel_count"].astype(int)
    out["total_parcel_area_acres"] = out["total_parcel_area_acres"].astype(float)
    out["median_parcel_area_acres"] = out["median_parcel_area_acres"].astype(float)

    return out


def compute_zoning_area_shares(zoning_dissolved: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Compute zoning polygon area (acres) and share of jurisdiction land area
    using dissolved zoning polygons (already filtered by jurisdiction).
    """
    if zoning_dissolved.empty:
        return pd.DataFrame(columns=["zoning_label", "zoning_area_acres", "pct_jurisdiction_land_area"])

    if "zoning_label" not in zoning_dissolved.columns:
        raise ValueError("Expected 'zoning_label' on dissolved zoning GeoDataFrame.")

    z = zoning_dissolved.copy()
    z_area = z.to_crs(WORK_CRS_EPSG)
    z_area["zoning_area_m2"] = z_area.geometry.area
    z_area["zoning_area_acres"] = z_area["zoning_area_m2"] * M2_TO_ACRES

    total_acres = float(z_area["zoning_area_acres"].sum() or 0.0)
    z_area["pct_jurisdiction_land_area"] = (z_area["zoning_area_acres"] / total_acres) if total_acres > 0 else 0.0

    return z_area[["zoning_label", "zoning_area_acres", "pct_jurisdiction_land_area"]].copy()


def build_tooltip(has_desc: bool, *, metric_short_label: str, metric_unit: str) -> dict[str, Any]:
    unit_suffix = "%" if metric_unit == "percent" else (" acres" if metric_unit == "acres" else "")
    metric_line = f"<b>{metric_short_label}:</b> {{metric_value}}{unit_suffix}<br/>"

    if has_desc:
        html = (
            "<b>Zoning:</b> {zoning_label}<br/>"
            "<b>Description:</b> {zoning_desc}<br/>"
            + metric_line +
            "<b>Parcels:</b> {parcel_count}<br/>"
            "<b>Total parcel acres:</b> {total_parcel_area_acres}<br/>"
            "<b>Median parcel acres:</b> {median_parcel_area_acres}<br/>"
            "<b>Zoning acres:</b> {zoning_area_acres}<br/>"
            "<b>% jur land:</b> {pct_jurisdiction_land_area_pct}%"
        )
    else:
        html = (
            "<b>Zoning:</b> {zoning_label}<br/>"
            + metric_line +
            "<b>Parcels:</b> {parcel_count}<br/>"
            "<b>Total parcel acres:</b> {total_parcel_area_acres}<br/>"
            "<b>Median parcel acres:</b> {median_parcel_area_acres}<br/>"
            "<b>Zoning acres:</b> {zoning_area_acres}<br/>"
            "<b>% jur land:</b> {pct_jurisdiction_land_area_pct}%"
        )

    return {"html": html, "style": {"backgroundColor": "white", "color": "black"}}


# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------
def main() -> None:
    configure_logging()

    st.set_page_config(page_title="Sarpy County Zoning Dashboard", layout="wide")
    st.title("Sarpy County Zoning Dashboard")
    st.caption(
        "Parcels joined to zoning districts; map colors by parcel count. "
        "Table includes parcel-area and zoning-area metrics."
    )

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

    metric_options = {
        "Parcel count": ("parcel_count", "Parcels", "count"),
        "Total parcel area (acres)": ("total_parcel_area_acres", "Parcel acres", "acres"),
        "Zoning polygon area (acres)": ("zoning_area_acres", "Zoning acres", "acres"),
        "% of jurisdiction land area": ("pct_jurisdiction_land_area", "% jur land", "percent"),
    }

    metric_label = st.sidebar.radio(
        "Choropleth metric",
        options=list(metric_options.keys()),
        index=0,
        help="Choose which metric drives map shading and table sorting.",
    )

    metric_col, metric_short_label, metric_unit = metric_options[metric_label]

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

    zoning_area = compute_zoning_area_shares(zoning_diss)
    map_gdf = map_gdf.merge(zoning_area, on="zoning_label", how="left")

    map_gdf["parcel_count"] = map_gdf["parcel_count"].fillna(0).astype(int)
    for c in ("total_parcel_area_acres", "median_parcel_area_acres", "zoning_area_acres", "pct_jurisdiction_land_area"):
        if c in map_gdf.columns:
            map_gdf[c] = map_gdf[c].fillna(0.0).astype(float)

    # KPIs (single 4-column row)
    total_parcels = int(parcels_f["parcel_id"].nunique()) if "parcel_id" in parcels_f.columns else len(parcels_f)
    matched_parcels = int(parcels_f["zoning_code"].notna().sum())
    unique_zones = int(rollups["zoning_label"].nunique())
    total_jur_acres = float(map_gdf["zoning_area_acres"].sum() or 0.0)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Parcels (filtered)", f"{total_parcels:,}")
    k2.metric(
        "Parcels w/ Zoning",
        f"{matched_parcels:,}",
        f"{(matched_parcels / total_parcels):.2%}" if total_parcels else "0%",
    )
    k3.metric("Zoning Codes (filtered)", f"{unique_zones:,}")
    k4.metric("Jurisdiction Land Area (acres)", f"{total_jur_acres:,.1f}")

    st.divider()

    left, right = st.columns([2, 1], gap="large")

    with right:
        st.subheader("Top Zoning Codes")

        # Start from the full non-geometry table
        table_df = map_gdf.drop(columns="geometry", errors="ignore").copy()

        # Derived display column (percent)
        table_df["pct_jurisdiction_land_area_pct"] = (table_df["pct_jurisdiction_land_area"] * 100).round(2)

        # Round other display fields
        table_df["total_parcel_area_acres"] = table_df["total_parcel_area_acres"].round(2)
        table_df["median_parcel_area_acres"] = table_df["median_parcel_area_acres"].round(3)
        table_df["zoning_area_acres"] = table_df["zoning_area_acres"].round(2)

        cols = [
            "zoning_label",
            "parcel_count",
            "total_parcel_area_acres",
            "median_parcel_area_acres",
            "zoning_area_acres",
            "pct_jurisdiction_land_area_pct",
        ]
        if "zoning_desc" in table_df.columns:
            cols.insert(1, "zoning_desc")

        # Sort by selected metric (mapping pct to pct-display column)
        sort_col = metric_col
        if sort_col == "pct_jurisdiction_land_area":
            sort_col = "pct_jurisdiction_land_area_pct"

        st.dataframe(
            table_df[cols].sort_values(sort_col, ascending=False).head(25).reset_index(drop=True),
            use_container_width=True,
            height=700,
        )

        st.caption("Tip: % jur land is based on dissolved zoning polygon area within the selected jurisdiction(s).")

    with left:
        st.subheader(f"Choropleth: {metric_label} by Zoning")

        map_gdf = add_fill_color(map_gdf, metric_col=metric_col)

        # Make tooltip fields human-friendly (rounding + percent)
        map_gdf["total_parcel_area_acres"] = map_gdf["total_parcel_area_acres"].round(2)
        map_gdf["median_parcel_area_acres"] = map_gdf["median_parcel_area_acres"].round(3)
        map_gdf["zoning_area_acres"] = map_gdf["zoning_area_acres"].round(2)
        map_gdf["pct_jurisdiction_land_area"] = map_gdf["pct_jurisdiction_land_area"].round(4)
        map_gdf["pct_jurisdiction_land_area_pct"] = (map_gdf["pct_jurisdiction_land_area"] * 100).round(2)

        # Add a display field for the selected metric (so tooltip is clear)
        if metric_col == "pct_jurisdiction_land_area":
            map_gdf["metric_value"] = (map_gdf[metric_col] * 100).round(2)
        else:
            map_gdf["metric_value"] = map_gdf[metric_col].round(2)

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
            tooltip=build_tooltip(
                has_desc=("zoning_desc" in map_gdf.columns),
                metric_short_label=metric_short_label,
                metric_unit=metric_unit,
            ),
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
            st.write("Jurisdiction labels:", {j: labels.get(j, f"Jurisdiction {j}") for j in selected_jurisdictions})

    st.divider()
    st.caption(
        "Notes: Parcel rollups are computed from parcels_with_zoning_1to1.parquet and filtered to selected jurisdiction(s). "
        "Zoning polygons are dissolved within the filter to keep the map readable. "
        "Areas are computed in a projected CRS (EPSG:26914) for accuracy."
    )


if __name__ == "__main__":
    main()