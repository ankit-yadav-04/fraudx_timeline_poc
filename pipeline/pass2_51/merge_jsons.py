import json
from pathlib import Path
from datetime import datetime


def parse_date_safe(item):
    """
    Supports both keys:
    - event_date (pass2_5 style)
    - date (older style)
    Invalid/missing dates go to end.
    """
    d = item.get("event_date") or item.get("date")
    if not d:
        return (1, datetime.max, "")
    try:
        return (0, datetime.strptime(d, "%Y-%m-%d"), d)
    except ValueError:
        return (1, datetime.max, d)


def merge_timelines(input_files):
    merged_events_by_date = []

    for file_path in input_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        events = data.get("events_by_date", [])
        if not isinstance(events, list):
            continue

        # Direct append exactly as requested (no dedup, no regrouping)
        merged_events_by_date.extend(events)

    # Sort by date, keep duplicates as separate entries
    merged_events_by_date.sort(key=parse_date_safe)

    return {"events_by_date": merged_events_by_date}


if __name__ == "__main__":
    input_files = [
        "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input1_pass2_5.json",
        "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input2_pass2_5.json",
        "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input3_pass2_5.json",
    ]

    output_file = "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/merged_pass2_5.json"

    merged = merge_timelines(input_files)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"Merged {len(input_files)} files.")
    print(f"Total events_by_date entries: {len(merged['events_by_date'])}")
    print(f"Saved to: {output_file}")
