from __future__ import annotations

import os
import json
from pathlib import Path

import shapely
import geopandas as gpd
import pandas as pd
import pydeck as pdk
import streamlit as st


PROCESSED_DIR = Path("data/processed")

PARCELS_PATH = PROCESSED_DIR / "parcels_with_zoning_1to1.parquet"
ZONING_PATH = PROCESSED_DIR / "zoning.parquet"  # non-dissolved (has jurisdiction + code/desc)


st.set_page_config(page_title="Sarpy County Zoning Dashboard", layout="wide")


@st.cache_data(show_spinner=True)
def load_parcels(_mtime: float) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(PARCELS_PATH)
    gdf = gdf.set_crs(4326) if gdf.crs is None else gdf.to_crs(4326)
    return gdf


@st.cache_data(show_spinner=True)
def load_zoning(_mtime: float) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(ZONING_PATH)
    gdf = gdf.set_crs(4326) if gdf.crs is None else gdf.to_crs(4326)
    return gdf


def view_state_from_bounds(gdf: gpd.GeoDataFrame) -> pdk.ViewState:
    minx, miny, maxx, maxy = gdf.total_bounds
    return pdk.ViewState(
        latitude=(miny + maxy) / 2,
        longitude=(minx + maxx) / 2,
        zoom=9.5,
        pitch=0,
    )


def add_fill_color(map_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = map_gdf.copy()

    # Always create parcel_count and ensure it's numeric
    if "parcel_count" not in out.columns:
        out["parcel_count"] = 0

    out["parcel_count"] = pd.to_numeric(out["parcel_count"], errors="coerce").fillna(0).astype(int)

    # Handle empty frame safely
    if len(out) == 0:
        out["fill_color"] = []
        return out

    max_count = out["parcel_count"].max()
    if max_count <= 0:
        max_count = 1

    def alpha_from_count(x: int) -> int:
        a = 60 + int(175 * (x / max_count))
        return max(60, min(235, a))

    out["fill_color"] = out["parcel_count"].apply(lambda x: [30, 120, 200, alpha_from_count(x)])
    return out


def _repair_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Repair invalid geometries to avoid dissolve/union TopologyException.
    """
    gdf = gdf.copy()
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    # Shapely 2.x: make_valid
    if hasattr(shapely, "make_valid"):
        gdf["geometry"] = gdf["geometry"].apply(
            lambda geom: shapely.make_valid(geom) if geom is not None else geom
        )

    # buffer(0) fallback (fix self-intersections)
    try:
        gdf["geometry"] = gdf["geometry"].buffer(0)
    except Exception:
        def _buf0(geom):
            try:
                return geom.buffer(0)
            except Exception:
                return geom
        gdf["geometry"] = gdf["geometry"].apply(_buf0)

    return gdf


def dissolve_zoning(zoning_filtered: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Dissolve zoning polygons by zoning_code within the filtered set.
    Repairs geometry before dissolve to prevent GEOS TopologyException.
    """
    z = zoning_filtered.copy()

    if "zoning_code" not in z.columns:
        raise ValueError("Expected zoning_code in zoning.parquet (from ZONECLASS).")

    z["zoning_label"] = z["zoning_code"].astype(str)

    keep = ["zoning_label", "geometry"]
    if "zoning_desc" in z.columns:
        keep.append("zoning_desc")

    z = z[keep].copy()
    z = z.set_crs(4326) if z.crs is None else z.to_crs(4326)

    # Description lookup BEFORE dissolve
    desc_lookup = None
    if "zoning_desc" in z.columns:
        desc_lookup = (
            z[["zoning_label", "zoning_desc"]]
            .dropna()
            .drop_duplicates("zoning_label")
        )

    # Work in projected CRS for robust union
    WORK_CRS = 26914  # NAD83 / UTM zone 14N (good for Sarpy County area ops)
    z_work = z.to_crs(WORK_CRS)

    # Repair geometry then dissolve
    z_work = _repair_geometry(z_work)
    dissolved = z_work[["zoning_label", "geometry"]].dissolve(by="zoning_label", as_index=False)

    # Back to WGS84 for mapping
    dissolved = dissolved.to_crs(4326)

    if desc_lookup is not None:
        dissolved = dissolved.merge(desc_lookup, on="zoning_label", how="left")

    return dissolved

def parse_jurisdiction_labels() -> dict[int, str]:
    """
    Parse JURISDICTION_LABELS from .env
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

st.title("Sarpy County Zoning Dashboard")
st.caption("Parcels joined to zoning districts; choropleth shows parcel concentration by zoning code.")

if not PARCELS_PATH.exists():
    st.error(f"Missing {PARCELS_PATH}. Build it with scripts/04_build_spatial_joins.py and scripts/05_dedup_parcels_with_zoning.py.")
    st.stop()

if not ZONING_PATH.exists():
    st.error(f"Missing {ZONING_PATH}. Build it with scripts/02_build_processed.py.")
    st.stop()

parcels = load_parcels(PARCELS_PATH.stat().st_mtime)
zoning = load_zoning(ZONING_PATH.stat().st_mtime)

# Sidebar: jurisdiction filter
st.sidebar.header("Filters")

if "jurisdiction" not in zoning.columns:
    st.sidebar.warning("No jurisdiction field found in zoning.parquet; filter disabled.")
    selected_jurisdictions = None
else:
    # show sorted unique ints
    labels = parse_jurisdiction_labels()

    jvals = sorted([int(x) for x in zoning["jurisdiction"].dropna().unique()])

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
zoning_f = zoning.copy()
if selected_jurisdictions is not None:
    zoning_f = zoning_f[zoning_f["jurisdiction"].isin(selected_jurisdictions)].copy()

# Dissolve within filter (robust map layer)
try:
    zoning_diss = dissolve_zoning(zoning_f)
except Exception as e:
    st.error(f"Failed to dissolve zoning polygons: {e}")
    st.stop()

# Filter parcels to zoning codes present in filtered zoning set
if "zoning_code" not in parcels.columns:
    st.error("Expected zoning_code in parcels_with_zoning_1to1.parquet")
    st.stop()

allowed_codes = set(zoning_f["zoning_code"].dropna().astype(str).unique())
parcels_f = parcels[parcels["zoning_code"].astype(str).isin(allowed_codes)].copy()

# Rollups from filtered parcels
rollups = (
    parcels_f.dropna(subset=["zoning_code"])
    .groupby("zoning_code")["parcel_id"]
    .nunique()
    .reset_index()
    .rename(columns={"zoning_code": "zoning_label", "parcel_id": "parcel_count"})
)
rollups["zoning_label"] = rollups["zoning_label"].astype(str)

# Merge counts to dissolved polygons
zoning_diss["zoning_label"] = zoning_diss["zoning_label"].astype(str)
map_gdf = zoning_diss.merge(rollups, on="zoning_label", how="left")
map_gdf["parcel_count"] = map_gdf["parcel_count"].fillna(0).astype(int)

# KPIs
total_parcels = int(parcels_f["parcel_id"].nunique())
matched_parcels = int(parcels_f["zoning_code"].notna().sum())
unique_zones = int(rollups["zoning_label"].nunique())

k1, k2, k3 = st.columns(3)
k1.metric("Parcels (filtered)", f"{total_parcels:,}")
k2.metric("Parcels w/ Zoning", f"{matched_parcels:,}", f"{matched_parcels / total_parcels:.2%}" if total_parcels else "0%")
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

    tooltip = {
        "html": "<b>Zoning:</b> {zoning_label}<br/>"
                "<b>Description:</b> {zoning_desc}<br/>"
                "<b>Parcels:</b> {parcel_count}",
        "style": {"backgroundColor": "white", "color": "black"},
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state_from_bounds(map_gdf),
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    )

    st.pydeck_chart(deck, use_container_width=True)

with st.expander("Debug"):
    st.write("Selected jurisdictions:", selected_jurisdictions)
    st.write("Zoning polygons (filtered):", len(zoning_f))
    st.write("Dissolved zoning codes:", len(zoning_diss))
    st.write("Max parcel_count:", int(map_gdf["parcel_count"].max() or 0))
    st.write(map_gdf[["zoning_label", "parcel_count"]].sort_values("parcel_count", ascending=False).head(10))
    st.write(
        "Jurisdiction labels:",
        {j: labels.get(j, f"Jurisdiction {j}") for j in selected_jurisdictions},
    )
st.divider()
st.caption(
    "Notes: Counts are computed from parcels_with_zoning_1to1.parquet and filtered to the selected jurisdiction(s). "
    "Zoning polygons are dissolved within the filter to keep the map readable."
)