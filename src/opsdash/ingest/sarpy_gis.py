from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import hashlib
import json
import math
import requests
from typing import Any, Dict, Optional

from opsdash.config import settings

RAW_DIR = Path("data/raw/sarpy_gis")


# -------------------------
# Utilities
# -------------------------
def utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def post_json(url: str, data: dict, timeout: int = 120) -> dict:
    r = requests.post(url, data=data, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_stream_to_file(url: str, out_path: Path, timeout: int = 180) -> None:
    ensure_dir(out_path.parent)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with out_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def snake_case(s: str) -> str:
    return (
        s.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .lower()
    )


# -------------------------
# ArcGIS Layer Downloader
# -------------------------
@dataclass
class ArcGisLayerIngestResult:
    name: str
    layer_url: str
    query_url: str
    output_path: str
    retrieved_at: str
    objectid_count: int
    batch_size: int
    max_record_count: int
    features_written: int
    sha256: str


def ingest_arcgis_layer_to_geojson(
    *,
    layer_url: str,
    out_dir: Path,
    out_name: str,
    where: str = "1=1",
    out_sr: int = 4326,
    batch_size: int = 200,
    timeout_meta: int = 60,
    timeout_ids: int = 120,
    timeout_batch: int = 180,
) -> ArcGisLayerIngestResult:
    """
    Robust ArcGIS ingestion for MapServer/FeatureServer layers:
      1) Fetch layer metadata
      2) Fetch all objectIds (returnIdsOnly)
      3) Fetch features by objectIds in small batches via POST
      4) Stream to a single GeoJSON FeatureCollection on disk
    """
    if not layer_url:
        raise ValueError(f"Missing layer_url for {out_name}")

    ensure_dir(out_dir)
    query_url = f"{layer_url}/query"

    # 1) metadata
    meta = post_json(layer_url, {"f": "pjson"}, timeout=timeout_meta)
    max_rc = int(meta.get("maxRecordCount", 1000))

    # 2) ids
    ids_resp = post_json(
        query_url,
        {"where": where, "returnIdsOnly": "true", "f": "pjson"},
        timeout=timeout_ids,
    )
    object_ids = ids_resp.get("objectIds") or []
    if not object_ids:
        raise RuntimeError(f"No objectIds returned for {out_name}. Check endpoint or permissions.")

    object_ids = sorted(object_ids)
    total_ids = len(object_ids)

    # keep conservative to avoid proxy/body limits; also honor max_rc if smaller
    bs = min(batch_size, max_rc) if max_rc > 0 else batch_size
    n_batches = math.ceil(total_ids / bs)

    out_path = out_dir / f"{out_name}.geojson"
    features_written = 0

    # 3–4) stream GeoJSON
    with out_path.open("w", encoding="utf-8") as f:
        f.write('{"type":"FeatureCollection","features":[\n')
        first = True

        for i in range(n_batches):
            batch = object_ids[i * bs : (i + 1) * bs]

            resp = post_json(
                query_url,
                {
                    "objectIds": ",".join(map(str, batch)),
                    "outFields": "*",
                    "outSR": str(out_sr),
                    "f": "geojson",
                },
                timeout=timeout_batch,
            )

            feats = resp.get("features", [])
            for feat in feats:
                if not first:
                    f.write(",\n")
                f.write(json.dumps(feat))
                first = False

            features_written += len(feats)
            print(f"{out_name}: batch {i+1}/{n_batches} +{len(feats)} (total {features_written}/{total_ids})")

        f.write("\n]}\n")

    return ArcGisLayerIngestResult(
        name=out_name,
        layer_url=layer_url,
        query_url=query_url,
        output_path=str(out_path),
        retrieved_at=utc_now_iso(),
        objectid_count=total_ids,
        batch_size=bs,
        max_record_count=max_rc,
        features_written=features_written,
        sha256=sha256_file(out_path),
    )


# -------------------------
# Hub Download URL Ingestion
# -------------------------
@dataclass
class DownloadIngestResult:
    name: str
    download_url: str
    output_path: str
    retrieved_at: str
    sha256: str


def ingest_download_geojson(
    *,
    download_url: str,
    out_dir: Path,
    out_name: str,
    timeout: int = 300,
) -> DownloadIngestResult:
    """
    Downloads GeoJSON directly (e.g., ArcGIS Hub /downloads/data?format=geojson...).
    """
    if not download_url:
        raise ValueError(f"Missing download_url for {out_name}")

    ensure_dir(out_dir)
    out_path = out_dir / f"{out_name}.geojson"

    print(f"{out_name}: downloading GeoJSON from {download_url}")
    get_stream_to_file(download_url, out_path, timeout=timeout)

    return DownloadIngestResult(
        name=out_name,
        download_url=download_url,
        output_path=str(out_path),
        retrieved_at=utc_now_iso(),
        sha256=sha256_file(out_path),
    )


# -------------------------
# Orchestrator
# -------------------------
def ingest_sarpy_all_available() -> Dict[str, Path]:
    """
    Ingest Sarpy GIS layers into data/raw/sarpy_gis/<YYYY-MM-DD>/.

    Required:
      - SARPY_PARCELS_LAYER_URL (ArcGIS layer URL)

    Optional:
      - SARPY_ZONING_LAYER_URL (ArcGIS layer URL)
      - SARPY_NEIGHBORHOODS_LAYER_URL (ArcGIS layer URL)
      - SARPY_NEIGHBORHOODS_DOWNLOAD_URL (direct GeoJSON download; used if set)
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = RAW_DIR / today
    ensure_dir(out_dir)

    manifest: Dict[str, Any] = {
        "retrieved_at": utc_now_iso(),
        "outputs": {},
        "notes": [
            "ArcGIS layer ingests use POST + objectId batching + streamed GeoJSON to avoid URL-length limits.",
            "If a Hub download URL is provided for neighborhoods, it is preferred over REST layer scraping.",
        ],
    }

    outputs: Dict[str, Path] = {}

    # --- Parcels (required, ArcGIS layer)
    parcels_res = ingest_arcgis_layer_to_geojson(
        layer_url=getattr(settings, "SARPY_PARCELS_LAYER_URL", ""),
        out_dir=out_dir,
        out_name="sarpy_tax_parcels",
        batch_size=200,
    )
    outputs["parcels"] = Path(parcels_res.output_path)
    manifest["outputs"]["parcels"] = asdict(parcels_res)

    # --- Zoning (optional, ArcGIS layer)
    zoning_url = getattr(settings, "SARPY_ZONING_LAYER_URL", "")
    if zoning_url:
        zoning_res = ingest_arcgis_layer_to_geojson(
            layer_url=zoning_url,
            out_dir=out_dir,
            out_name="sarpy_zoning",
            batch_size=200,
        )
        outputs["zoning"] = Path(zoning_res.output_path)
        manifest["outputs"]["zoning"] = asdict(zoning_res)

    # --- Neighborhoods (optional; prefer direct download if provided)
    n_download_url = getattr(settings, "SARPY_NEIGHBORHOODS_DOWNLOAD_URL", "")
    n_layer_url = getattr(settings, "SARPY_NEIGHBORHOODS_LAYER_URL", "")

    if n_download_url:
        n_res = ingest_download_geojson(
            download_url=n_download_url,
            out_dir=out_dir,
            out_name="sarpy_neighborhoods",
        )
        outputs["neighborhoods"] = Path(n_res.output_path)
        manifest["outputs"]["neighborhoods"] = asdict(n_res)
    elif n_layer_url:
        n_res = ingest_arcgis_layer_to_geojson(
            layer_url=n_layer_url,
            out_dir=out_dir,
            out_name="sarpy_neighborhoods",
            batch_size=200,
        )
        outputs["neighborhoods"] = Path(n_res.output_path)
        manifest["outputs"]["neighborhoods"] = asdict(n_res)

    # --- Write manifest
    (out_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))

    return outputs