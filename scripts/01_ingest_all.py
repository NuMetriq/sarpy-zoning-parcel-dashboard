"""
A script to ingest all available Sarpy GIS data.
"""

from __future__ import annotations

import logging
from dotenv import load_dotenv

from opsdash.common import configure_logging
from opsdash.ingest.sarpy_gis import ingest_sarpy_all_available

LOGGER = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    load_dotenv()

    LOGGER.info("Starting ingestion...")
    outputs = ingest_sarpy_all_available()

    LOGGER.info("Ingestion complete.")
    for key, value in outputs.items():
        LOGGER.info("  %s: %s", key, value)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())