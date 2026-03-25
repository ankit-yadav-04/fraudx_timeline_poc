import json
import csv
from pathlib import Path


def pass3_json_to_csv(input_json_path: str, output_csv_path: str) -> None:
    input_path = Path(input_json_path)
    output_path = Path(output_csv_path)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for day in data.get("events_by_date", []):
        event_date = day.get("event_date")
        for idx, event in enumerate(day.get("events", []), start=1):
            tags = event.get("event_tags", [])
            details = event.get("event_details", [])
            flags = event.get("event_flags", [])

            rows.append(
                {
                    "event_date": event_date,
                    "event_index_on_date": idx,
                    "event_tags": "|".join(tags),  # tag1|tag2
                    "event_details": " || ".join(details),  # detail1 || detail2
                    "flag_count": len(flags),
                    "flag_types": "|".join([f.get("flag_type", "") for f in flags]),
                    "flag_severities": "|".join([f.get("severity", "") for f in flags]),
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_date",
                "event_index_on_date",
                "event_tags",
                "event_details",
                "flag_count",
                "flag_types",
                "flag_severities",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV saved: {output_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    pass3_json_to_csv(
        input_json_path="/home/ankit/smartsense_code/fraudx_timeline_poc/workflow/jsons/all_xray_run_002/pass3_output.json",
        output_csv_path="/home/ankit/smartsense_code/fraudx_timeline_poc/to_csv/all_xray_run_002/pass3_output.csv",
    )
