import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


INPUT_JSON_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons/after_validation_01/pass2_output.json"
OUTPUT_CSV_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "to_csv/workflow2/timeline_after_validation_01.csv"
)


def parse_date_safe(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return datetime.max


def load_events_by_date(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        events_by_date = data.get("events_by_date", [])
    elif isinstance(data, list):
        events_by_date = data
    else:
        raise ValueError("Unsupported JSON structure.")

    if not isinstance(events_by_date, list):
        raise ValueError("'events_by_date' must be a list.")

    clean = [x for x in events_by_date if isinstance(x, dict)]
    clean.sort(key=lambda d: parse_date_safe(str(d.get("date", ""))))
    return clean


def compute_max_sizes(events_by_date: List[Dict[str, Any]]) -> tuple[int, int]:
    max_events = 0
    max_details = 0

    for day in events_by_date:
        events = day.get("events", [])
        if not isinstance(events, list):
            continue

        max_events = max(max_events, len(events))

        for ev in events:
            if not isinstance(ev, dict):
                continue
            details = ev.get("event_details", [])
            if isinstance(details, list):
                max_details = max(max_details, len(details))

    return max_events, max_details


def build_header(max_events: int, max_details: int) -> List[str]:
    header = ["date"]

    for i in range(1, max_events + 1):
        header.append(f"event_{i}_tag")
        header.append(f"event_{i}_chunk_ids")  # joined by ^^
        for j in range(1, max_details + 1):
            header.append(f"event_{i}_detail_{j}")

    return header


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").strip()


def build_row(day: Dict[str, Any], max_events: int, max_details: int) -> List[str]:
    row: List[str] = [safe_text(day.get("date", ""))]
    events = day.get("events", [])
    if not isinstance(events, list):
        events = []

    for idx in range(max_events):
        if idx < len(events) and isinstance(events[idx], dict):
            ev = events[idx]
            tag = safe_text(ev.get("event_tag", ""))

            chunk_ids = ev.get("chunk_ids", [])
            if isinstance(chunk_ids, list):
                chunk_ids_str = "^^".join(
                    safe_text(c) for c in chunk_ids if safe_text(c)
                )
            else:
                chunk_ids_str = ""

            details = ev.get("event_details", [])
            if not isinstance(details, list):
                details = []
            details = [safe_text(d) for d in details]

            row.append(tag)
            row.append(chunk_ids_str)

            for j in range(max_details):
                row.append(details[j] if j < len(details) else "")
        else:
            # empty columns for missing event slots
            row.append("")  # event_i_tag
            row.append("")  # event_i_chunk_ids
            row.extend([""] * max_details)

    return row


def json_to_wide_csv(input_json: str, output_csv: str) -> None:
    events_by_date = load_events_by_date(input_json)
    max_events, max_details = compute_max_sizes(events_by_date)

    header = build_header(max_events=max_events, max_details=max_details)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for day in events_by_date:
            writer.writerow(
                build_row(day, max_events=max_events, max_details=max_details)
            )

    print(f"Wrote CSV: {output_path}")
    print(f"Rows (dates): {len(events_by_date)}")
    print(f"Max events in a date: {max_events}")
    print(f"Max detail lines in an event: {max_details}")


if __name__ == "__main__":
    json_to_wide_csv(INPUT_JSON_PATH, OUTPUT_CSV_PATH)
