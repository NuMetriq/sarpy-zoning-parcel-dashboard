# Sarpy Zoning Parcel Dashboard

![Status](https://img.shields.io/badge/status-public%20release-green)
![Release](https://img.shields.io/badge/release-v1.1.0-blue)

A reproducible GIS analytics pipeline and interactive dashboard that shows how parcels are distributed across zoning districts within Sarpy County, Nebraska and its municipalities.

This project uses publicly available ArcGIS data published by Sarpy County to demonstrate an end-to-end workflow: ingestion, validation, spatial joins, geometry repair, aggregation, and visualization.

**Built by NuMetriq LLC** using publicly available Sarpy County GIS data.


## Project Purpose

Local governments maintain rich GIS datasets, but answering even basic structural questions—such as “How many parcels are governed by each zoning designation?”—often requires significant manual GIS work.

This project provides:

- a clean, auditable parcel-to-zoning assignment
- jurisdiction-aware aggregation
- an interactive map and summary table suitable for planning, reporting, and exploratory analysis

The goal is to demonstrate how public GIS data can be transformed into **decision-ready operational insight** using transparent, reproducible, open-source tooling.


## What’s New in v1.1.0

- **Parcel area-based zoning metrics**
  - Total parcel area by zoning code
  - Median parcel size by zoning code
  - Percentage of jurisdiction land area by zoning

- **Zoning metric toggle (count vs land area)**
  - Allow users to switch how zoning intensity is measured
    - Parcel count (v1.0.0)
    - Total land area (v1.1.0)

- **Jurisdiction comparison mode**
  - Side-by-side zoning distributions across jurisdictions (e.g., Bellevue, Papillion, La Vista)
  - Zoning mix by **parcel count**
  - Zoning mix by **land area (acres)**
  - Optional Top-N zoning categories with remainder grouped as “Other”

- **Data Quality & Coverage panel**
  - Parcel coverage KPIs (in scope, assigned zoning, % assigned)
  - Geometry health indicators (empty / invalid geometries)
  - Coverage breakdown by jurisdiction

- **CSV export for filtered zoning results
  - Allow users to export zoning summaries and parcel subsets that reflect the current dashboard filters

- **Map layer visibility controls**
  - Toggle zoning fill, outlines, and labels independently


## What the Dashboard Shows

### Zoning districts, colored by parcel concentration

- Each polygon represents a zoning district, not individual parcels.
- Color intensity reflects the number of parcels primarily governed by that zoning code.
- Darker areas indicate zoning categories that affect more parcels.

**Note on land-area metrics:**  
“Zoning polygon area (acres)” and “% of jurisdiction land area” represent the same underlying quantity expressed in different units. As a result, they typically produce identical map patterns.

### Deduplicated, authoritative parcel counts

- Parcels are spatially joined to zoning polygons.
- Parcels overlapping multiple zoning districts are assigned to the district with the largest area of overlap.
- Each parcel is counted once and only once.

This avoids double-counting and reflects the dominant zoning assignment for each parcel.

### Jurisdiction-based filtering

Users can filter by jurisdiction (e.g., Bellevue, Papillion, La Vista)

When a jurisdiction is selected:

- Zoning polygons are filtered first
- Parcels are filtered to relevant zoning codes
- Counts, colors, and summaries update consistently

This ensures that parcel counts and zoning geometry remain internally consistent within each selected jurisdiction.

### Tabular summary

A ranked table shows:

- zoning code
- zoning description
- parcel count

This supports reporting, exporting, and downstream analysis.

### Jurisdiction comparison

A dedicated **Comparison** tab enables side-by-side analysis of zoning structure across jurisdictions.

Users can compare:
- zoning mix by **parcel count**
- zoning mix by **land area (acres)**

Distributions can be shown as absolute values or percent share, and the view respects the active jurisdiction filters.

### Data quality & coverage

A **Data Quality & Coverage** panel makes data completeness explicit.

It surfaces:
- how many parcels are in scope
- how many parcels received a zoning assignment
- geometry validity checks
- coverage metrics by jurisdiction

This helps users interpret results correctly and identify gaps or edge cases in the source data.

### Map layer controls

The map includes visibility toggles for:
- zoning polygon fills
- zoning outlines
- zoning labels

This allows users to simplify the view or focus on structure without altering the underlying metrics.


## Methodology at a Glance

1. Ingest public parcel and zoning layers from Sarpy County ArcGIS REST services  
2. Normalize geometries and enforce a common CRS  
3. Spatially join parcels to zoning polygons  
4. Resolve multi-zoning overlaps using dominant area assignment  
5. Aggregate parcel counts by zoning code and jurisdiction  
6. Visualize results in an interactive Streamlit dashboard


## What This Dashboard Does *Not* Do (by design)

- ❌ It does not evaluate zoning policy quality or compliance
- ❌ It does not show land value, land use, or population
- ❌ It does not replace detailed parcel-level GIS workflows

It answers a structural inventory question, not a normative policy question.


## Example use cases

- Understanding which zoning categories affect the most properties
- Scoping the impact of zoning code updates
- Planning staff workload estimation
- Sanity-checking zoning datasets against parcel reality
- Preparing inputs for deeper land-use or housing analysis



## Data Sources

All data used in this project is publicly available.

- Sarpy County GIS - Tax Parcels
- Sarpy County GIS - Zoning Districts
- ArcGIS REST services published by Sarpy County

No proprietary or restricted data is used.


### Data Disclaimer

This project reflects the structure of the published GIS datasets at the time of ingestion.
Parcel counts and zoning boundaries are subject to change as source data is updated by the county.


## Technical Overview

### Tech Stack

- Python
- GeoPandas / Shapely
- Pandas
- Streamlit
- PyDeck (deck.gl)
- ArcGIS REST API

### Key design decisions

- Local projected CRS (UTM14N) is used for all area calculations to ensure geometric correctness.
- Geometry repair (`make_valid`, `buffer(0)`) is applied defensively to prevent topology errors.
- Aggregations are driven from parcel-level truth, not polygon overlaps.
- The app is configuration-driven (e.g., jurisdiction labels via `.env`).



## Repository Structure


```text
sarpy-zoning-parcel-dashboard/
├── data/
│   ├── raw/                # Raw GeoJSON pulled from ArcGIS
│   └── processed/          # Cleaned Parquet datasets
├── scripts/
│   ├── 01_ingest_*.py      # Data ingestion from ArcGIS REST
│   ├── 02_build_processed.py
│   ├── 04_build_spatial_joins.py
│   ├── 05_dedup_parcels_with_zoning.py
│   ├── 07_build_zoning_dissolve.py
│   └── 09_audit_arcgis_layer_fields.py
├── src/
│   └── opsdash/
│       └── app/
│           └── streamlit_app.py
├── .env.example
├── README.md
└── pyproject.toml
```

Raw and processed GIS datasets are intentionally excluded from version control and are fully reproducible using the included ingestion scripts and public ArcGIS REST services.


## Setup & Running the Dashboard

### 1) Clone the repository

```powershell
git clone https://github.com/NuMetriq/sarpy-zoning-parcel-dashboard.git
cd sarpy-zoning-parcel-dashboard
```

### 2) Create and activate a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1 #Windows
```

### 3) Install dependencies

```powershell
pip install .
```

### 4) Configure environment variables

Copy the example file:

```powershell
cp .env.example .env
```

Edit `.env` to include jurisdiction labels:

```ini
JURISDICTION_LABELS=10:Bellevue,20:Gretna,30:La Vista,40:Papillion,50:Springfield,60:Unincorporated
```

### 5) Run the pipeline

```powershell
python scripts/01_ingest_all.py
python scripts/02_build_processed.py
python scripts/04_build_spatial_joins.py
python scripts/05_dedup_parcels_with_zoning.py
python scripts/07_build_zoning_dissolve.py
```

### 6) Launch the dashboard

```powershell
streamlit run src/opsdash/app/streamlit_app.py
```

## Quick Start

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install .
python scripts/01_ingest_all.py
python scripts/02_build_processed.py
python scripts/04_build_spatial_joins.py
python scripts/05_dedup_parcels_with_zoning.py
python scripts/07_build_zoning_dissolve.py
streamlit run src/opsdash/app/streamlit_app.py
```


## Dashboard Preview

### Zoning distribution (all jurisdictions)
![All jurisdictions](docs/screenshots/all_jurisdictions.png)

### Bellevue zoning distribution
![Bellevue](docs/screenshots/bellevue_only.png)

### Zoning tooltip detail
![Tooltip](docs/screenshots/tooltip.png)



## Data Integrity Notes

- Parcel counts are computed from `parcels_with_zoning_1to1.parquet`
- Zoning polygons are dissolved after filtering, not before
- Invalid geometries are repaired prior to union operations
- Empty or invalid user selections are handled gracefully


## Future Enhancements

Potential next steps include:

- parcel-area-weighted zoning impact analysis
- historical zoning change tracking
- land-use, housing, or valuation overlays
- jurisdiction-level time series
- performance optimizations for large zoning label sets


## About This Project

This project was built to demonstrate:

- applied GIS engineering
- data quality handling
- spatial reasoning
- reproducible analytics workflows
- practical local-government use cases

It is intentionally scoped to be useful, transparent, and extensible, rather than a toy visualization.


## License

This project is licensed under the MIT License.
See the `LICENSE` file for details.


## About NuMetriq

NuMetriq LLC builds transparent, reproducible analytics tools that help public-sector and SMB organizations turn complex data into clear operational insight.

Our work emphasizes:
- data quality and validation
- spatial and operational analytics
- auditable, decision-support workflows

This project was developed independently using public data and is intended to demonstrate NuMetriq’s approach to applied analytics in a local-government context.

## Contact

NuMetriq LLC -- Public-sector and operational analytics

Email: kentrb@numetriq.org

Website: numetriq.org
