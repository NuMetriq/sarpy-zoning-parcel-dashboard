"""
Audit ArcGIS layer fields
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv

from opsdash.common import configure_logging

LOGGER = logging.getLogger(__name__)
DEFAULT_TIMEOUT_S = 60


def post_form(url: str, data: dict[str, Any], timeout_s: int = DEFAULT_TIMEOUT_S) -> dict[str, Any]:
    resp = requests.post(url, data=data, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    configure_logging()
    load_dotenv()

    url = os.getenv("SARPY_ZONING_LAYER_URL", "").strip()
    if not url:
        raise ValueError("Set SARPY_ZONING_LAYER_URL in .env")

    meta = post_form(url, {"f": "pjson"})
    fields = meta.get("fields", []) or []

    LOGGER.info("Layer: %s", meta.get("name"))
    LOGGER.info("Geometry: %s", meta.get("geometryType"))
    LOGGER.info("Field count: %s", len(fields))

    for f in fields:
        LOGGER.info("  %s (%s)", f.get("name"), f.get("type"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())