import os
from dotenv import load_dotenv
import requests

load_dotenv()

def post_json(url: str, data: dict, timeout: int = 60) -> dict:
    r = requests.post(url, data=data, timeout=timeout)
    r.raise_for_status()
    return r.json()

def main():
    url = os.getenv("SARPY_ZONING_LAYER_URL", "")
    if not url:
        raise ValueError("Set SARPY_ZONING_LAYER_URL in .env")

    meta = post_json(url, {"f": "pjson"})
    fields = meta.get("fields", [])

    print("Layer:", meta.get("name"))
    print("Geometry:", meta.get("geometryType"))
    print("Field count:", len(fields))
    print("\nFields:")
    for f in fields:
        print(f"  {f.get('name')}  ({f.get('type')})")

if __name__ == "__main__":
    main()