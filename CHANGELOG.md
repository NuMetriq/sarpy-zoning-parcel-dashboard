\# Changelog



All notable changes to this project are documented in this file.



The format follows \[Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

and this project adheres to \[Semantic Versioning](https://semver.org/).



---



\## \[v1.1.0] — 2026-01-22



\### Added

\- Jurisdiction \*\*Comparison\*\* tab for side-by-side zoning analysis

&nbsp; - Zoning mix by parcel count

&nbsp; - Zoning mix by land area (acres)

&nbsp; - Percent-share and absolute views

\- \*\*Data Quality \& Coverage\*\* panel exposing:

&nbsp; - parcel coverage metrics

&nbsp; - geometry health indicators

&nbsp; - jurisdiction-level completeness

\- Map layer visibility controls (fill, outlines, labels)



\### Improved

\- Metric clarity for land-area-based choropleths

\- Interpretability of zoning distributions across municipalities

\- UI responsiveness for exploratory analysis



\### Fixed

\- Guardrails for missing or inferred parcel jurisdiction

\- Geometry robustness during spatial joins and dissolves



---



\## \[v1.0.1] — 2026-01-19



Refactored code according to Python best practices.



---



\## \[v1.0.0] — Initial Public Release



\### Added

\- End-to-end GIS ingestion and processing pipeline

\- Parcel-to-zoning spatial joins with deduplication

\- Jurisdiction-aware zoning rollups

\- Interactive Streamlit dashboard with choropleth map and tables

