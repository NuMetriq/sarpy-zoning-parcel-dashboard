from dotenv import load_dotenv
load_dotenv()

from opsdash.ingest.sarpy_gis import ingest_sarpy_all_available

def main():
    outputs = ingest_sarpy_all_available()
    print("Ingestion complete:")
    for k, v in outputs.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    main()