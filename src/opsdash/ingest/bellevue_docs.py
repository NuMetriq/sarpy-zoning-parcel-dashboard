from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw/bellevue_docs")

def ingest_bellevue_docs():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = RAW_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = out_dir / "MANIFEST.txt"
    manifest.write_text(
        "City of Bellevue documents ingestion placeholder\n"
        f"retrieved_at={datetime.utcnow().isoformat()}Z\n"
    )

    return out_dir