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


def add_fill_color(map_gdf: gpd.GeoDataFrame, *, metric_col: str) -> gpd.GeoDataFrame:
    """
    Color polygons by selected metric using quantile bins (robust to skew/outliers).
    Expects metric_col to be numeric and in display units (e.g., percent already * 100).
    """
    out = map_gdf.copy()
    vals = pd.to_numeric(out.get(metric_col, 0), errors="coerce").fillna(0.0)

    if out.empty:
        out["fill_color"] = []
        return out

    # If everything is the same, use a single color
    if float(vals.max()) == float(vals.min()):
        out["fill_color"] = [[120, 120, 120, 180]] * len(out)
        return out

    # Quantile binning (0..5). duplicates="drop" avoids errors when many ties.
    bins = pd.qcut(vals.rank(method="average"), q=6, labels=False, duplicates="drop")
    bins = bins.fillna(0).astype(int)

    # 6-step ramp. Light -> Dark.
    palette = [
        [247, 252, 245, 200],
        [199, 233, 192, 200],
        [116, 196, 118, 200],
        [49, 163, 84, 200],
        [0, 109, 44, 200],
        [0, 68, 27, 200],
    ]

    out["fill_color"] = bins.apply(lambda b: palette[min(int(b), len(palette) - 1)])
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


def compute_zoning_area_by_jurisdiction(zoning_filtered: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Compute zoning polygon area (acres) by (jurisdiction, zoning_code) using raw zoning polygons.
    This is used for side-by-side comparison across jurisdictions.

    NOTE: If zoning polygons overlap within a jurisdiction, areas will double-count. This mirrors
    the underlying geometry as published.
    """
    required = {"jurisdiction", "zoning_code", "geometry"}
    missing = required - set(zoning_filtered.columns)
    if missing:
        raise ValueError(f"Zoning polygons missing required columns: {sorted(missing)}")

    z = zoning_filtered.dropna(subset=["jurisdiction", "zoning_code"]).copy()
    if z.empty:
        return pd.DataFrame(columns=["jurisdiction", "zoning_label", "zoning_area_acres"])

    z_area = z.to_crs(WORK_CRS_EPSG)
    z_area["area_m2"] = z_area.geometry.area
    z_area["area_acres"] = z_area["area_m2"] * M2_TO_ACRES

    out = (
        z_area.groupby([z_area["jurisdiction"].astype(int), z_area["zoning_code"].astype(str)], dropna=False)["area_acres"]
        .sum()
        .reset_index()
        .rename(columns={"zoning_code": "zoning_label", "area_acres": "zoning_area_acres"})
    )
    return out


@st.cache_data(show_spinner=True)
def assign_parcel_jurisdiction(
    _parcels: gpd.GeoDataFrame,
    _zoning_raw: gpd.GeoDataFrame,
    parcels_mtime: float,
    zoning_mtime: float,
) -> gpd.GeoDataFrame:
    """
    Ensure _parcels have a 'jurisdiction' column.

    - If _parcels already include 'jurisdiction', returns a copy.
    - Otherwise, performs a spatial join against zoning polygons to infer jurisdiction.

    This is cached because the join can be expensive on large datasets.
    """
    if "jurisdiction" in _parcels.columns:
        return _parcels.copy()

    required = {"parcel_id", "geometry"}
    missing = required - set(_parcels.columns)
    if missing:
        raise ValueError(f"Parcels missing required columns for jurisdiction assignment: {sorted(missing)}")

    if "jurisdiction" not in _zoning_raw.columns:
        raise ValueError("Cannot infer parcel jurisdiction: zoning.parquet has no 'jurisdiction' field.")

    # Work in projected CRS for robust spatial ops
    p = ensure_crs(_parcels, WGS84_EPSG).to_crs(WORK_CRS_EPSG)[["parcel_id", "geometry"]].copy()
    z = ensure_crs(_zoning_raw, WGS84_EPSG).to_crs(WORK_CRS_EPSG)[["jurisdiction", "geometry"]].copy()

    # Repair zoning geometry to avoid topology errors during sjoin
    z = repair_geometry(z)

    try:
        joined = gpd.sjoin(p, z, how="left", predicate="intersects")
    except TypeError:
        # Older geopandas uses `op=` instead of `predicate=`
        joined = gpd.sjoin(p, z, how="left", op="intersects")

    # If a parcel hits multiple zoning polys (rare), keep the first non-null jurisdiction.
    j = (
        joined.dropna(subset=["jurisdiction"])
        .groupby("parcel_id", as_index=False)["jurisdiction"]
        .first()
    )

    out = _parcels.copy()
    out = out.merge(j, on="parcel_id", how="left")
    return out

def build_tooltip(has_desc: bool, *, metric_short_label: str, metric_unit: str) -> dict[str, Any]:
    unit_suffix = "%" if metric_unit == "percent" else (" acres" if metric_unit == "acres" else "")
    metric_line = f"<b>{metric_short_label}:</b> {{metric_value}}{unit_suffix}<br/>"

    if has_desc:
        html = (
            "<b>Zoning:</b> {zoning_label}<br/>"
            "<b>Description:</b> {zoning_desc}<br/>"
            + metric_line
            + "<b>Parcels:</b> {parcel_count}<br/>"
            "<b>Total parcel acres:</b> {total_parcel_area_acres}<br/>"
            "<b>Median parcel acres:</b> {median_parcel_area_acres}<br/>"
            "<b>Zoning acres:</b> {zoning_area_acres}<br/>"
            "<b>% jur land:</b> {pct_jurisdiction_land_area_pct}%"
        )
    else:
        html = (
            "<b>Zoning:</b> {zoning_label}<br/>"
            + metric_line
            + "<b>Parcels:</b> {parcel_count}<br/>"
            "<b>Total parcel acres:</b> {total_parcel_area_acres}<br/>"
            "<b>Median parcel acres:</b> {median_parcel_area_acres}<br/>"
            "<b>Zoning acres:</b> {zoning_area_acres}<br/>"
            "<b>% jur land:</b> {pct_jurisdiction_land_area_pct}%"
        )

    return {"html": html, "style": {"backgroundColor": "white", "color": "black"}}


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert a DataFrame to CSV bytes (UTF-8) for Streamlit downloads."""
    return df.to_csv(index=False).encode("utf-8")


def make_safe_filename(s: str) -> str:
    """Make a safe-ish filename token."""
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s).strip("_")


def _format_jurisdiction(j: int, labels: dict[int, str]) -> str:
    return labels.get(int(j), f"Jurisdiction {int(j)}")


def _maybe_import_altair():
    try:
        import altair as alt  # type: ignore

        return alt
    except Exception:
        return None


# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------
def main() -> None:
    configure_logging()

    st.set_page_config(page_title="Sarpy County Zoning Dashboard", layout="wide")
    st.title("Sarpy County Zoning Dashboard")
    st.caption(
        "Parcels joined to zoning districts; choropleth can be driven by count, area, percent, or density/intensity metrics. "
        "Table includes parcel-area and zoning-area metrics. "
        "Use the Comparison tab for side-by-side zoning distributions across jurisdictions."
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
        "Parcel density (parcels / zoning acre)": ("parcels_per_zoning_acre", "Parcels per acre", "rate"),
        "Parcel intensity (parcel acres / zoning acre)": ("parcel_acres_per_zoning_acre", "Acres per acre", "rate"),
        "Median parcel size (acres)": ("median_parcel_area_acres", "Median parcel acres", "acres"),
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
            format_func=lambda j: _format_jurisdiction(int(j), labels),
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
    for c in (
        "total_parcel_area_acres",
        "median_parcel_area_acres",
        "zoning_area_acres",
        "pct_jurisdiction_land_area",
    ):
        if c in map_gdf.columns:
            map_gdf[c] = map_gdf[c].fillna(0.0).astype(float)

    denom = map_gdf["zoning_area_acres"].replace(0.0, pd.NA)

    # parcels per zoning acre (density)
    map_gdf["parcels_per_zoning_acre"] = (map_gdf["parcel_count"] / denom).fillna(0.0)

    # parcel acres per zoning acre (intensity proxy)
    map_gdf["parcel_acres_per_zoning_acre"] = (map_gdf["total_parcel_area_acres"] / denom).fillna(0.0)

    # Ensure choropleth metric is in display units
    if metric_col == "pct_jurisdiction_land_area":
        map_gdf["metric_for_color"] = map_gdf["pct_jurisdiction_land_area"] * 100.0
    else:
        map_gdf["metric_for_color"] = pd.to_numeric(map_gdf.get(metric_col, 0), errors="coerce").fillna(0.0)

    # KPIs (single 4-column row)
    total_parcels = int(parcels_f["parcel_id"].nunique()) if "parcel_id" in parcels_f.columns else len(parcels_f)
    matched_parcels = int(parcels_f["zoning_code"].notna().sum())
    unique_zones = int(rollups["zoning_label"].nunique())
    total_jur_acres = float(map_gdf["zoning_area_acres"].sum() or 0.0)

    # -------------------------------------------------------------------
    # Tabs
    # -------------------------------------------------------------------
    tab_map, tab_compare = st.tabs(["Map & Rollups", "Comparison"])

    # ==========================
    # Tab: Map & Rollups (existing)
    # ==========================
    with tab_map:
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

            # Display percent
            table_df["pct_jurisdiction_land_area_pct"] = (table_df["pct_jurisdiction_land_area"] * 100).round(2)

            # Round display fields
            table_df["total_parcel_area_acres"] = table_df["total_parcel_area_acres"].round(2)
            table_df["median_parcel_area_acres"] = table_df["median_parcel_area_acres"].round(3)
            table_df["zoning_area_acres"] = table_df["zoning_area_acres"].round(2)
            table_df["parcels_per_zoning_acre"] = table_df["parcels_per_zoning_acre"].round(4)
            table_df["parcel_acres_per_zoning_acre"] = table_df["parcel_acres_per_zoning_acre"].round(4)

            cols = [
                "zoning_label",
                "parcel_count",
                "total_parcel_area_acres",
                "median_parcel_area_acres",
                "zoning_area_acres",
                "pct_jurisdiction_land_area_pct",
                "parcels_per_zoning_acre",
                "parcel_acres_per_zoning_acre",
            ]
            if "zoning_desc" in table_df.columns:
                cols.insert(1, "zoning_desc")

            # Sort by selected metric
            sort_col = metric_col
            if sort_col == "pct_jurisdiction_land_area":
                sort_col = "pct_jurisdiction_land_area_pct"

            st.dataframe(
                table_df[cols].sort_values(sort_col, ascending=False).head(25).reset_index(drop=True),
                use_container_width=True,
                height=700,
            )

            st.subheader("Exports")

            export_rollups = table_df[cols].copy()

            parcel_cols = ["parcel_id", "zoning_code"]
            if "zoning_desc" in parcels_f.columns:
                parcel_cols.append("zoning_desc")
            if "jurisdiction" in parcels_f.columns:
                parcel_cols.append("jurisdiction")

            export_parcels = parcels_f.drop(columns="geometry", errors="ignore")[parcel_cols].copy()
            export_parcels["zoning_code"] = export_parcels["zoning_code"].astype(str)

            rollups_name = make_safe_filename(metric_label.lower())
            if selected_jurisdictions is None:
                jur_token = "all_jurisdictions"
            else:
                jur_token = "jur_" + "_".join(map(str, selected_jurisdictions))

            rollups_filename = f"zoning_rollups_{jur_token}_{rollups_name}.csv"
            parcels_filename = f"parcels_filtered_{jur_token}.csv"

            st.caption(f"Rollups rows: {len(export_rollups):,} • Parcels rows: {len(export_parcels):,}")

            st.download_button(
                label="Download zoning rollups (CSV)",
                data=df_to_csv_bytes(export_rollups),
                file_name=rollups_filename,
                mime="text/csv",
                help="Exports the rollup table shown above, reflecting current filters and metrics.",
            )

            st.download_button(
                label="Download filtered parcels (CSV)",
                data=df_to_csv_bytes(export_parcels),
                file_name=parcels_filename,
                mime="text/csv",
                help="Exports parcel IDs and zoning codes for the current filter selection.",
            )

            st.caption("Tip: % jur land is based on dissolved zoning polygon area within the selected jurisdiction(s).")

        with left:
            st.subheader(f"Choropleth: {metric_label} by Zoning")

            # Fill colors based on metric_for_color (display units)
            map_gdf_colored = add_fill_color(map_gdf, metric_col="metric_for_color")

            # Tooltip fields
            map_gdf_colored["total_parcel_area_acres"] = map_gdf_colored["total_parcel_area_acres"].round(2)
            map_gdf_colored["median_parcel_area_acres"] = map_gdf_colored["median_parcel_area_acres"].round(3)
            map_gdf_colored["zoning_area_acres"] = map_gdf_colored["zoning_area_acres"].round(2)
            map_gdf_colored["pct_jurisdiction_land_area_pct"] = (map_gdf_colored["pct_jurisdiction_land_area"] * 100).round(2)

            map_gdf_colored["metric_value"] = (
                pd.to_numeric(map_gdf_colored["metric_for_color"], errors="coerce").fillna(0.0).round(2)
            )

            geojson = json.loads(map_gdf_colored.to_json())

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
                initial_view_state=view_state_from_bounds(map_gdf_colored),
                tooltip=build_tooltip(
                    has_desc=("zoning_desc" in map_gdf_colored.columns),
                    metric_short_label=metric_short_label,
                    metric_unit=metric_unit,
                ),
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            )

            st.pydeck_chart(deck, use_container_width=True, key=f"map-{metric_label}")

            st.caption(
                "Note: 'Zoning polygon area' and '% of jurisdiction land area' are equivalent scalings, so they will look identical."
            )

        with st.expander("Debug"):
            st.write("Selected jurisdictions:", selected_jurisdictions)
            st.write("Zoning polygons (filtered):", len(zoning_f))
            st.write("Dissolved zoning codes:", len(zoning_diss))
            st.write("Selected metric:", metric_col)

            metric_cols = [
                "parcel_count",
                "total_parcel_area_acres",
                "median_parcel_area_acres",
                "zoning_area_acres",
                "pct_jurisdiction_land_area",
                "parcels_per_zoning_acre",
                "parcel_acres_per_zoning_acre",
            ]

            st.write("Metric summary:")
            st.dataframe(map_gdf[metric_cols].describe().T)

            rank_df = map_gdf.set_index("zoning_label")[metric_cols].rank(method="average")
            st.write("Rank correlation (1.0 = identical ordering):")
            st.dataframe(rank_df.corr())

            st.write(
                "Metric_for_color min/max:",
                float(map_gdf["metric_for_color"].min()),
                float(map_gdf["metric_for_color"].max()),
            )
            st.dataframe(
                map_gdf[["zoning_label", "metric_for_color"]]
                .sort_values("metric_for_color", ascending=False)
                .head(10)
                .reset_index(drop=True)
            )

        st.divider()
        st.caption(
            "Notes: Parcel rollups are computed from parcels_with_zoning_1to1.parquet and filtered to selected jurisdiction(s). "
            "Zoning polygons are dissolved within the filter to keep the map readable. "
            "Areas are computed in a projected CRS (EPSG:26914) for accuracy."
        )

    # ==========================
    # Tab: Comparison (NEW)
    # ==========================
    with tab_compare:
        st.subheader("Comparison: Zoning Mix by Jurisdiction")
        st.caption(
            "Side-by-side zoning distributions across jurisdictions. "
            "This view respects the jurisdiction filter in the sidebar; you can further select which of those to compare."
        )

        if selected_jurisdictions is None:
            st.info("Jurisdiction comparison requires zoning.parquet to include a 'jurisdiction' field.")
            st.stop()

        # Comparison selection is a subset of the already-filtered jurisdictions
        compare_jurs = st.multiselect(
            "Jurisdictions to compare",
            options=selected_jurisdictions,
            default=selected_jurisdictions,
            format_func=lambda j: _format_jurisdiction(int(j), labels),
        )

        if len(compare_jurs) == 0:
            st.warning("Select at least one jurisdiction to compare.")
            st.stop()

        metric_choice = st.radio(
            "Comparison metric",
            options=["Zoning mix by parcel count", "Zoning mix by land area"],
            index=0,
            horizontal=True,
        )

        display_choice = st.radio(
            "Display",
            options=["Percent share", "Absolute"],
            index=0,
            horizontal=True,
            help="Percent share shows composition within each jurisdiction.",
        )

        top_n = st.slider("Top N zoning categories", min_value=5, max_value=40, value=15, step=1)
        group_other = st.checkbox("Group the remainder into 'Other'", value=True)

        zoning_compare = zoning_f[zoning_f["jurisdiction"].isin(compare_jurs)].copy()

        # Metric: land area (from zoning polygons)
        if metric_choice == "Zoning mix by land area":
            area_df = compute_zoning_area_by_jurisdiction(zoning_compare)
            if area_df.empty:
                st.warning("No zoning polygons found for the selected jurisdictions.")
                st.stop()

            area_df["jurisdiction_label"] = area_df["jurisdiction"].apply(lambda j: _format_jurisdiction(int(j), labels))
            value_col = "zoning_area_acres"
            units_label = "Acres"
            mix_df = area_df.rename(columns={"zoning_label": "zoning_label"}).copy()

        # Metric: parcel count (from parcels; infer jurisdiction if missing)
        else:
            try:
                parcels_with_j = assign_parcel_jurisdiction(
                    parcels_f,
                    zoning_compare,
                    parcels_mtime=float(PARCELS_PATH.stat().st_mtime),
                    zoning_mtime=float(ZONING_RAW_PATH.stat().st_mtime),
                )
            except Exception as exc:
                st.error(f"Failed to compute parcel-count comparison: {exc}")
                st.stop()

            if parcels_with_j["jurisdiction"].isna().all():
                st.warning(
                    "Could not infer parcel jurisdiction (no spatial matches). "
                    "Try widening the jurisdiction filter or verify geometry alignment."
                )
                st.stop()

            p = parcels_with_j.dropna(subset=["jurisdiction", "zoning_code"]).copy()
            p = p[p["jurisdiction"].astype(int).isin([int(x) for x in compare_jurs])].copy()

            if p.empty:
                st.warning("No parcels found for the selected jurisdictions.")
                st.stop()

            mix_df = (
                p.groupby([p["jurisdiction"].astype(int), p["zoning_code"].astype(str)], dropna=False)["parcel_id"]
                .nunique()
                .reset_index()
                .rename(columns={"zoning_code": "zoning_label", "parcel_id": "parcel_count"})
            )
            mix_df["jurisdiction_label"] = mix_df["jurisdiction"].apply(lambda j: _format_jurisdiction(int(j), labels))
            value_col = "parcel_count"
            units_label = "Parcels"

        # Optionally reduce categories to Top N + Other (for readability)
        totals_by_zone = mix_df.groupby("zoning_label")[value_col].sum().sort_values(ascending=False)
        top_zones = set(totals_by_zone.head(top_n).index)

        if group_other:
            mix_df["zoning_group"] = mix_df["zoning_label"].where(mix_df["zoning_label"].isin(top_zones), "Other")
        else:
            mix_df["zoning_group"] = mix_df["zoning_label"].where(mix_df["zoning_label"].isin(top_zones))

        mix_df = mix_df.dropna(subset=["zoning_group"]).copy()

        plot_df = (
            mix_df.groupby(["jurisdiction_label", "zoning_group"], as_index=False)[value_col]
            .sum()
            .rename(columns={"zoning_group": "zoning"})
        )

        # Add share for each jurisdiction
        plot_df["jur_total"] = plot_df.groupby("jurisdiction_label")[value_col].transform("sum")
        plot_df["share"] = (plot_df[value_col] / plot_df["jur_total"]).fillna(0.0)

        # Table (always)
        st.markdown("### Data")
        show_df = plot_df.copy()
        show_df[value_col] = show_df[value_col].round(2)
        show_df["share_pct"] = (show_df["share"] * 100).round(2)
        st.dataframe(
            show_df.sort_values(["jurisdiction_label", value_col], ascending=[True, False]).reset_index(drop=True),
            use_container_width=True,
            height=360,
        )

        st.markdown("### Chart")
        alt = _maybe_import_altair()
        if alt is None:
            st.info("Altair is not available in this environment. Showing table only.")
        else:
            y_field = "share" if display_choice == "Percent share" else value_col
            y_title = "Share" if display_choice == "Percent share" else units_label

            tooltip_fields = [
                alt.Tooltip("jurisdiction_label:N", title="Jurisdiction"),
                alt.Tooltip("zoning:N", title="Zoning"),
                alt.Tooltip(f"{value_col}:Q", title=units_label, format=",.2f" if value_col != "parcel_count" else ",d"),
                alt.Tooltip("share:Q", title="Share", format=".1%"),
            ]

            chart = (
                alt.Chart(plot_df)
                .mark_bar()
                .encode(
                    x=alt.X("jurisdiction_label:N", title="Jurisdiction"),
                    y=alt.Y(f"{y_field}:Q", title=y_title, stack="normalize" if display_choice == "Percent share" else "zero"),
                    color=alt.Color("zoning:N", title="Zoning"),
                    tooltip=tooltip_fields,
                )
                .properties(height=450)
            )

            st.altair_chart(chart, use_container_width=True)

        # Exports for comparison
        st.markdown("### Export")
        metric_token = "parcel_count" if value_col == "parcel_count" else "land_area"
        compare_token = "compare_" + "_".join(map(str, compare_jurs))
        export_name = f"jurisdiction_zoning_mix_{metric_token}_{compare_token}.csv"
        st.download_button(
            label="Download comparison data (CSV)",
            data=df_to_csv_bytes(show_df.drop(columns=["jur_total"], errors="ignore")),
            file_name=export_name,
            mime="text/csv",
        )


if __name__ == "__main__":
    main()