from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _load_env() -> None:
    """
    Load environment variables from .env as early as possible.

    - First tries .env in the current working directory (repo root when run normally).
    - Then searches upward starting from this file's location, so it still works
      even if the working directory is different.
    """
    load_dotenv(override=False)

    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            load_dotenv(candidate, override=False)
            break


_load_env()


@dataclass(frozen=True)
class Settings:
    """
    Central configuration for the project.
    Values come from environment variables (optionally sourced from .env).
    """

    # Required for Sarpy ingestion
    SARPY_PARCELS_LAYER_URL: str = ""

    # Optional
    SARPY_ZONING_LAYER_URL: str = ""
    SARPY_NEIGHBORHOODS_LAYER_URL: str = ""
    SARPY_NEIGHBORHOODS_DOWNLOAD_URL: str = ""

    # Optional (legacy)
    SARPY_STREETS_URL: str = ""

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            SARPY_PARCELS_LAYER_URL=os.getenv("SARPY_PARCELS_LAYER_URL", "").strip(),
            SARPY_ZONING_LAYER_URL=os.getenv("SARPY_ZONING_LAYER_URL", "").strip(),
            SARPY_NEIGHBORHOODS_LAYER_URL=os.getenv("SARPY_NEIGHBORHOODS_LAYER_URL", "").strip(),
            SARPY_NEIGHBORHOODS_DOWNLOAD_URL=os.getenv("SARPY_NEIGHBORHOODS_DOWNLOAD_URL", "").strip(),
            SARPY_STREETS_URL=os.getenv("SARPY_STREETS_URL", "").strip(),
        )

    def get_required(self, key: str) -> str:
        value = getattr(self, key, "")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Missing required setting: {key}. "
                f"Set it in your environment or in a .env file at the repo root."
            )
        return value.strip()


settings = Settings.from_env()