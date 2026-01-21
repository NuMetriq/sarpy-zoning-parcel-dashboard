from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

LOGGER = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/bellevue_docs")


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class BellevueDocsIngestResult:
    name: str
    output_dir: str
    retrieved_at: str
    notes: list[str]


def ingest_bellevue_docs(*, out_root: Path = RAW_DIR) -> Path:
    """
    Placeholder ingest for City of Bellevue documents.

    Creates:
      data/raw/bellevue_docs/<YYYY-MM-DD>/MANIFEST.json
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = out_root / today
    ensure_dir(out_dir)

    result = BellevueDocsIngestResult(
        name="bellevue_docs_placeholder",
        output_dir=str(out_dir),
        retrieved_at=utc_now_iso(),
        notes=[
            "Placeholder ingestion. Replace with real document fetching (PDFs, HTML, etc.).",
            "Manifest format mirrors Sarpy GIS ingestion for consistency.",
        ],
    )

    manifest: Dict[str, Any] = {
        "retrieved_at": result.retrieved_at,
        "outputs": {"bellevue_docs": asdict(result)},
    }

    manifest_path = out_dir / "MANIFEST.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    LOGGER.info("Wrote: %s", manifest_path)
    return out_dir