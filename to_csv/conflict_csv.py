import csv
import json
from pathlib import Path
from typing import Any, Dict, List


INPUT_JSON_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "workflow2/jsons/all_xray_run_workflow2_002_10/conflicts.json"
)
OUTPUT_CSV_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "to_csv/workflow2/conflicts_002_10.csv"
)


def load_conflicts(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected conflicts JSON as top-level list.")
    return [c for c in data if isinstance(c, dict)]


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").strip()


def compute_max_sizes(conflicts: List[Dict[str, Any]]) -> tuple[int, int]:
    """
    Returns:
    - max_dates_involved_count
    - max_source_events_count
    """
    max_dates = 0
    max_source_events = 0

    for conflict in conflicts:
        dates_involved = conflict.get("dates_involved", [])
        source_events = conflict.get("source_events", [])

        if isinstance(dates_involved, list):
            max_dates = max(max_dates, len(dates_involved))

        if isinstance(source_events, list):
            max_source_events = max(max_source_events, len(source_events))

    return max_dates, max_source_events


def build_header(max_dates: int, max_source_events: int) -> List[str]:
    header = [
        "conflict_id",
        "conflict_type",
        "severity",
        "conflict_brief",
    ]

    # Expand dates_involved into separate columns
    for i in range(1, max_dates + 1):
        header.append(f"date_involved_{i}")

    # Expand source_events into separate column groups
    for i in range(1, max_source_events + 1):
        header.extend(
            [
                f"source_event_{i}_date",
                f"source_event_{i}_chunk_ids",  # joined by ^^
                f"source_event_{i}_event",
            ]
        )

    return header


def build_row(
    conflict: Dict[str, Any],
    max_dates: int,
    max_source_events: int,
) -> List[str]:
    row: List[str] = [
        safe_text(conflict.get("conflict_id", "")),
        safe_text(conflict.get("conflict_type", "")),
        safe_text(conflict.get("severity", "")),
        safe_text(conflict.get("conflict_brief", "")),
    ]

    dates_involved = conflict.get("dates_involved", [])
    if not isinstance(dates_involved, list):
        dates_involved = []

    for i in range(max_dates):
        row.append(safe_text(dates_involved[i]) if i < len(dates_involved) else "")

    source_events = conflict.get("source_events", [])
    if not isinstance(source_events, list):
        source_events = []

    for i in range(max_source_events):
        if i < len(source_events) and isinstance(source_events[i], dict):
            src = source_events[i]

            src_date = safe_text(src.get("date", ""))

            chunk_ids = src.get("chunk_ids", [])
            if isinstance(chunk_ids, list):
                chunk_ids_str = "^^".join(
                    safe_text(c) for c in chunk_ids if safe_text(c)
                )
            else:
                chunk_ids_str = ""

            src_event = safe_text(src.get("event", ""))

            row.extend([src_date, chunk_ids_str, src_event])
        else:
            row.extend(["", "", ""])

    return row


def conflicts_json_to_wide_csv(input_json: str, output_csv: str) -> None:
    conflicts = load_conflicts(input_json)

    max_dates, max_source_events = compute_max_sizes(conflicts)
    header = build_header(max_dates=max_dates, max_source_events=max_source_events)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for conflict in conflicts:
            writer.writerow(
                build_row(
                    conflict=conflict,
                    max_dates=max_dates,
                    max_source_events=max_source_events,
                )
            )

    print(f"Wrote CSV: {output_path}")
    print(f"Total conflicts: {len(conflicts)}")
    print(f"Max dates_involved count: {max_dates}")
    print(f"Max source_events count: {max_source_events}")


if __name__ == "__main__":
    conflicts_json_to_wide_csv(INPUT_JSON_PATH, OUTPUT_CSV_PATH)
