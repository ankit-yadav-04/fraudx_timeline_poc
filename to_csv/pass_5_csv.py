import json
import csv
from pathlib import Path


def pass5_json_to_csv(input_json_path: str, output_csv_path: str) -> None:
    input_path = Path(input_json_path)
    output_path = Path(output_csv_path)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    contradictions = data.get("contradictions", [])
    if not isinstance(contradictions, list):
        raise ValueError("'contradictions' must be a list")

    rows = []
    for idx, c in enumerate(contradictions, start=1):
        detail_a = c.get("detail_a", {}) or {}
        detail_b = c.get("detail_b", {}) or {}
        dates_involved = c.get("dates_involved", []) or []

        rows.append(
            {
                "row_index": idx,
                "contradiction_id": c.get("contradiction_id", ""),
                "contradiction_type": c.get("contradiction_type", ""),
                "severity": c.get("severity", ""),
                "dates_involved": "|".join(dates_involved),  # YYYY-MM-DD|YYYY-MM-DD
                "detail_a_date": detail_a.get("date", ""),
                "detail_b_date": detail_b.get("date", ""),
                "explanation": c.get("explanation", ""),
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "row_index",
                "contradiction_id",
                "contradiction_type",
                "severity",
                "dates_involved",
                "detail_a_date",
                "detail_b_date",
                "explanation",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV saved: {output_path}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    pass5_json_to_csv(
        input_json_path="/home/ankit/smartsense_code/fraudx_timeline_poc/workflow/jsons/all_xray_run_002/pass5_output.json",
        output_csv_path="/home/ankit/smartsense_code/fraudx_timeline_poc/to_csv/all_xray_run_002/pass5_output.csv",
    )
