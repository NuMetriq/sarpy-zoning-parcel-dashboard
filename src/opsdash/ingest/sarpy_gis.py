from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests

from opsdash.config import settings

LOGGER = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/sarpy_gis")
DEFAULT_OUT_SR = 4326


# -------------------------
# Utilities
# -------------------------
def utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_layer_url(layer_url: str) -> str:
    """
    Ensure we have the ArcGIS layer *root* URL, not the /query endpoint.
    Example:
      input  .../FeatureServer/0/query  -> .../FeatureServer/0
      input  .../FeatureServer/0        -> unchanged
    """
    url = (layer_url or "").strip()
    if not url:
        return ""
    if url.endswith("/query"):
        url = url[: -len("/query")]
    return url.rstrip("/")


def post_form_json(
    session: requests.Session,
    url: str,
    data: dict[str, Any],
    *,
    timeout_s: int,
) -> dict[str, Any]:
    """
    ArcGIS endpoints often accept form-encoded POSTs. This returns parsed JSON.
    """
    resp = session.post(url, data=data, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


def stream_get_to_file(
    session: requests.Session,
    url: str,
    out_path: Path,
    *,
    timeout_s: int,
    chunk_bytes: int = 1024 * 1024,
) -> None:
    ensure_dir(out_path.parent)
    with session.get(url, stream=True, timeout=timeout_s) as resp:
        resp.raise_for_status()
        with out_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_bytes):
                if chunk:
                    f.write(chunk)


# -------------------------
# Results
# -------------------------
@dataclass(frozen=True)
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


@dataclass(frozen=True)
class DownloadIngestResult:
    name: str
    download_url: str
    output_path: str
    retrieved_at: str
    sha256: str


# -------------------------
# ArcGIS Layer Downloader
# -------------------------
def ingest_arcgis_layer_to_geojson(
    *,
    session: requests.Session,
    layer_url: str,
    out_dir: Path,
    out_name: str,
    where: str = "1=1",
    out_sr: int = DEFAULT_OUT_SR,
    batch_size: int = 200,
    timeout_meta_s: int = 60,
    timeout_ids_s: int = 120,
    timeout_batch_s: int = 180,
) -> ArcGisLayerIngestResult:
    """
    Robust ArcGIS ingestion for MapServer/FeatureServer layers:

      1) Fetch layer metadata (pjson)
      2) Fetch all objectIds (returnIdsOnly)
      3) Fetch features by objectIds in small batches via POST
      4) Stream to a single GeoJSON FeatureCollection on disk
    """
    layer_url = normalize_layer_url(layer_url)
    if not layer_url:
        raise ValueError(f"Missing layer_url for {out_name}")

    ensure_dir(out_dir)
    query_url = f"{layer_url}/query"

    # 1) metadata
    meta = post_form_json(session, layer_url, {"f": "pjson"}, timeout_s=timeout_meta_s)
    max_rc = int(meta.get("maxRecordCount", 1000) or 1000)

    # 2) ids
    ids_resp = post_form_json(
        session,
        query_url,
        {"where": where, "returnIdsOnly": "true", "f": "pjson"},
        timeout_s=timeout_ids_s,
    )
    object_ids: list[int] = ids_resp.get("objectIds") or []
    if not object_ids:
        raise RuntimeError(
            f"No objectIds returned for {out_name}. "
            f"Check the endpoint, permissions, or your where clause."
        )

    object_ids = sorted(object_ids)
    total_ids = len(object_ids)

    # keep conservative to avoid proxy/body limits; also honor max_rc if smaller
    bs = min(batch_size, max_rc) if max_rc > 0 else batch_size
    n_batches = math.ceil(total_ids / bs)

    out_path = out_dir / f"{out_name}.geojson"
    features_written = 0

    LOGGER.info("%s: %s objectIds, batch_size=%s (%s batches)", out_name, f"{total_ids:,}", bs, n_batches)

    # 3–4) stream GeoJSON
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write('{"type":"FeatureCollection","features":[\n')
        first = True

        for i in range(n_batches):
            batch = object_ids[i * bs : (i + 1) * bs]

            resp = post_form_json(
                session,
                query_url,
                {
                    "objectIds": ",".join(map(str, batch)),
                    "outFields": "*",
                    "outSR": str(out_sr),
                    "f": "geojson",
                },
                timeout_s=timeout_batch_s,
            )

            feats = resp.get("features") or []
            for feat in feats:
                if not first:
                    f.write(",\n")
                f.write(json.dumps(feat))
                first = False

            features_written += len(feats)
            LOGGER.info(
                "%s: batch %s/%s (+%s, total %s/%s)",
                out_name,
                i + 1,
                n_batches,
                len(feats),
                f"{features_written:,}",
                f"{total_ids:,}",
            )

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
def ingest_download_geojson(
    *,
    session: requests.Session,
    download_url: str,
    out_dir: Path,
    out_name: str,
    timeout_s: int = 300,
) -> DownloadIngestResult:
    """
    Downloads GeoJSON directly (e.g., ArcGIS Hub /downloads/data?format=geojson...).
    """
    url = (download_url or "").strip()
    if not url:
        raise ValueError(f"Missing download_url for {out_name}")

    ensure_dir(out_dir)
    out_path = out_dir / f"{out_name}.geojson"

    LOGGER.info("%s: downloading GeoJSON from %s", out_name, url)
    stream_get_to_file(session, url, out_path, timeout_s=timeout_s)

    return DownloadIngestResult(
        name=out_name,
        download_url=url,
        output_path=str(out_path),
        retrieved_at=utc_now_iso(),
        sha256=sha256_file(out_path),
    )


# -------------------------
# Orchestrator
# -------------------------
def ingest_sarpy_all_available(*, out_root: Path = RAW_DIR) -> Dict[str, Path]:
    """
    Ingest Sarpy GIS layers into data/raw/sarpy_gis/<YYYY-MM-DD>/.

    Required:
      - SARPY_PARCELS_LAYER_URL (ArcGIS layer root URL)

    Optional:
      - SARPY_ZONING_LAYER_URL (ArcGIS layer root URL)
      - SARPY_NEIGHBORHOODS_LAYER_URL (ArcGIS layer root URL)
      - SARPY_NEIGHBORHOODS_DOWNLOAD_URL (direct GeoJSON download; preferred if set)
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = out_root / today
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

    with requests.Session() as session:
        # --- Parcels (required)
        parcels_url = settings.get_required("SARPY_PARCELS_LAYER_URL")
        parcels_res = ingest_arcgis_layer_to_geojson(
            session=session,
            layer_url=parcels_url,
            out_dir=out_dir,
            out_name="sarpy_tax_parcels",
            batch_size=200,
        )
        outputs["parcels"] = Path(parcels_res.output_path)
        manifest["outputs"]["parcels"] = asdict(parcels_res)

        # --- Zoning (optional)
        zoning_url = normalize_layer_url(getattr(settings, "SARPY_ZONING_LAYER_URL", ""))
        if zoning_url:
            zoning_res = ingest_arcgis_layer_to_geojson(
                session=session,
                layer_url=zoning_url,
                out_dir=out_dir,
                out_name="sarpy_zoning",
                batch_size=200,
            )
            outputs["zoning"] = Path(zoning_res.output_path)
            manifest["outputs"]["zoning"] = asdict(zoning_res)

        # --- Neighborhoods (optional; prefer direct download)
        n_download_url = (getattr(settings, "SARPY_NEIGHBORHOODS_DOWNLOAD_URL", "") or "").strip()
        n_layer_url = normalize_layer_url(getattr(settings, "SARPY_NEIGHBORHOODS_LAYER_URL", ""))

        if n_download_url:
            n_res = ingest_download_geojson(
                session=session,
                download_url=n_download_url,
                out_dir=out_dir,
                out_name="sarpy_neighborhoods",
            )
            outputs["neighborhoods"] = Path(n_res.output_path)
            manifest["outputs"]["neighborhoods"] = asdict(n_res)
        elif n_layer_url:
            n_res = ingest_arcgis_layer_to_geojson(
                session=session,
                layer_url=n_layer_url,
                out_dir=out_dir,
                out_name="sarpy_neighborhoods",
                batch_size=200,
            )
            outputs["neighborhoods"] = Path(n_res.output_path)
            manifest["outputs"]["neighborhoods"] = asdict(n_res)

    (out_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    LOGGER.info("Wrote manifest: %s", out_dir / "MANIFEST.json")

    return outputs