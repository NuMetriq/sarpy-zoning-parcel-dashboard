from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    # Use env vars when possible; keep defaults empty until you paste URLs
    SARPY_PARCELS_LAYER_URL: str = os.getenv("SARPY_PARCELS_LAYER_URL", "")
    SARPY_ZONING_LAYER_URL: str = os.getenv("SARPY_ZONING_LAYER_URL", "")
    SARPY_NEIGHBORHOODS_DOWNLOAD_URL: str = os.getenv("SARPY_NEIGHBORHOODS_DOWNLOAD_URL", "")
    SARPY_STREETS_URL: str = os.getenv("SARPY_STREETS_URL", "")

settings = Settings()