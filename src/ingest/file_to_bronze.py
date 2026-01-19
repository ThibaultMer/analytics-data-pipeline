import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_DIR = PROJECT_ROOT / "data" / "input"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def ingest_file_to_bronze(file_path: Path) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_name = f"{file_path.stem}_{ts}{file_path.suffix}"
    out_path = BRONZE_DIR / out_name
    shutil.copy(file_path, out_path)
    return out_path


def main() -> None:
    for file_path in INPUT_DIR.iterdir():
        if file_path.is_file():
            out = ingest_file_to_bronze(file_path)
            print(f"[OK] Ingested {file_path.name} → {out.name}")


if __name__ == "__main__":
    main()
