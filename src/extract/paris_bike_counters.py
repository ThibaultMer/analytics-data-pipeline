import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://opendata.paris.fr/api/records/1.0/search/"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

@dataclass
class DatasetConfig:
    name: str
    dataset: str
    rows: int = 100
    max_pages: Optional[int] = None  # None = no limit


def http_get_json(params: Dict[str, Any]) -> Dict[str, Any]:
    url = BASE_URL + "?" + urlencode(params, doseq=True)
    req = Request(url, headers={"User-Agent": "analytics-data-pipeline/1.0"})
    with urlopen(req, timeout=60) as resp:
        raw = resp.read()
    return json.loads(raw)


def write_bronze(payload: Dict[str, Any], prefix: str, page: Optional[int] = None) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"_p{page:04d}" if page is not None else ""
    out = BRONZE_DIR / f"{prefix}{suffix}_{ts}.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def detect_date_field(sample_record_fields: Dict[str, Any]) -> Optional[str]:
    """
    Try to guess the date field name from the dataset record fields.
    We look for common patterns.
    """
    candidates = []
    for k, v in sample_record_fields.items():
        lk = k.lower()
        if "date" in lk or "time" in lk:
            # Keep only scalar-ish values that look like timestamps/strings
            if isinstance(v, (str, int, float)) or v is None:
                candidates.append(k)

    # Prefer the most likely names first
    priority = ["date", "dateheure", "datetime", "timestamp", "date_time", "date_heure"]
    for p in priority:
        for c in candidates:
            if c.lower() == p:
                return c

    return candidates[0] if candidates else None


def fetch_all_pages(cfg: DatasetConfig, extra_params: Dict[str, Any], prefix: str) -> List[Path]:
    """
    Fetch all records from an Opendatasoft dataset using pagination (start/rows).
    Saves each page as raw JSON into Bronze.
    """
    saved: List[Path] = []
    start = 0
    page = 1

    while True:
        params = {
            "dataset": cfg.dataset,
            "rows": cfg.rows,
            "start": start,
        }
        params.update(extra_params)

        payload = http_get_json(params)

        out = write_bronze(payload, prefix=prefix, page=page)
        saved.append(out)

        nhits = payload.get("nhits", 0)
        nrecords = len(payload.get("records", []))

        print(f"[INFO] {cfg.name}: page {page} | start={start} | records={nrecords} | nhits={nhits} | saved={out.name}")

        if nrecords == 0:
            break

        start += cfg.rows
        page += 1

        if start >= nhits:
            break

        if cfg.max_pages is not None and page > cfg.max_pages:
            print(f"[WARN] {cfg.name}: max_pages reached ({cfg.max_pages}). Stopping early.")
            break

    return saved


def build_counts_params_last_days(dataset: str, days: int = 30) -> Dict[str, Any]:
    """
    Try to build a 'where' filter for the last N days, if we can guess the date field.
    If not possible, fallback to sorting by the best date-like field and pulling a limited number of pages.
    """
    # Get a sample record to inspect available fields
    sample = http_get_json({"dataset": dataset, "rows": 1})
    records = sample.get("records", [])
    if not records:
        return {"sort": "-record_timestamp"}  # fallback

    fields = records[0].get("fields", {})
    date_field = detect_date_field(fields)
    # Opendatasoft provides a standard metadata field 'record_timestamp' sometimes
    # We'll try to use dataset's own date field if found, otherwise fallback.
    if date_field:
        # Use ISO date (YYYY-MM-DD) to keep it simple
        since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        # Opendatasoft "where" syntax often supports: <field> >= 'YYYY-MM-DD'
        return {
            "where": f"{date_field} >= '{since}'",
            "sort": f"-{date_field}",
        }

    # Fallback: sort by record_timestamp and take recent pages
    return {"sort": "-record_timestamp"}


def main() -> None:
    start_time = time.time()

    # Dataset 1: all counters (locations / metadata)
    counters_cfg = DatasetConfig(
        name="Paris bike counters (metadata)",
        dataset="comptage-velo-compteurs",
        rows=100,
        max_pages=None,  # fetch all
    )

    # Dataset 2: counts (hourly measurements). This can be huge, so we fetch recent window.
    counts_cfg = DatasetConfig(
        name="Paris bike counts (measurements)",
        dataset="comptage-velo-donnees-compteurs",
        rows=100,
        max_pages=10,  # safety limit if filter doesn't work; adjust later
    )

    print("[START] Issue #1 – Bronze extraction (Paris bike counters)")
    print("[INFO] Fetching ALL counters metadata…")
    counter_files = fetch_all_pages(
        counters_cfg,
        extra_params={},
        prefix="paris_bike_counters",
    )

    print("[INFO] Fetching RECENT counts (attempt last 30 days)…")
    counts_params = build_counts_params_last_days(counts_cfg.dataset, days=30)
    count_files = fetch_all_pages(
        counts_cfg,
        extra_params=counts_params,
        prefix="paris_bike_counts",
    )

    elapsed = time.time() - start_time
    print("[DONE]")
    print(f"[SUMMARY] counters pages: {len(counter_files)} | counts pages: {len(count_files)} | duration: {elapsed:.2f}s")
    print("[NEXT] Silver step will aggregate hourly → daily and join with counter locations for mapping.")


if __name__ == "__main__":
    main()
