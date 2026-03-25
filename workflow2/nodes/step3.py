"""Step 3: combine pass1 chunk outputs into date-level rows."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from loguru import logger
import time


DEFAULT_OUTPUT_DIR = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons"
DEFAULT_COMBINED_FILENAME = "combined_pass1_by_date.json"


def parse_date_safe(date_str: Any) -> datetime:
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d")
    except Exception:
        return datetime.max


def load_json_list(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"Expected JSON list at: {file_path}")

    return [row for row in data if isinstance(row, dict)]


def split_chunk_by_date(chunk_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert one pass1 chunk object:
      {
        "chunk": "...",
        "chunk_number": 1,
        "pageNumbers": [...],
        "events_by_date": [
          {"date": "YYYY-MM-DD", "events": [...]}
        ]
      }
    into rows:
      {
        "chunk": "...",
        "chunk_number": 1,
        "pageNumbers": [...],
        "date": "YYYY-MM-DD",
        "events": [...]
      }
    """
    output: List[Dict[str, Any]] = []

    chunk_id = chunk_obj.get("chunk")
    chunk_number = chunk_obj.get("chunk_number")
    page_numbers = chunk_obj.get("pageNumbers", [])
    events_by_date = chunk_obj.get("events_by_date", [])

    if not isinstance(events_by_date, list):
        return output

    for day in events_by_date:
        if not isinstance(day, dict):
            continue

        date_val = day.get("date")
        events = day.get("events", [])

        if not date_val or not isinstance(events, list) or len(events) == 0:
            continue

        output.append(
            {
                "chunk": chunk_id,
                "chunk_number": chunk_number,
                "pageNumbers": page_numbers if isinstance(page_numbers, list) else [],
                "date": date_val,
                "events": events,
            }
        )

    return output


def build_combined_output(input_files: List[str]) -> List[Dict[str, Any]]:
    combined: List[Dict[str, Any]] = []

    for file_path in input_files:
        rows = load_json_list(file_path)
        for chunk_obj in rows:
            combined.extend(split_chunk_by_date(chunk_obj))

    combined.sort(key=lambda item: parse_date_safe(item.get("date")))
    return combined


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def run_step3(
    step2_outputs: List[str],
    run_label: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    output_filename: str = DEFAULT_COMBINED_FILENAME,
) -> str:
    """
    Build combined pass1-by-date artifact and return output path.
    """
    combined = build_combined_output(step2_outputs)

    output_path = Path(output_dir) / run_label / output_filename
    save_json(output_path, combined)

    return str(output_path)


async def step3_combine_by_date_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected state keys:
    - step2_outputs: List[str]
    - run_label: str (optional, defaults to "run")
    - output_dir: str (optional)
    - step3_output_filename: str (optional)
    """
    step2_outputs = state.step2_outputs
    run_label = state.run_label
    output_dir = state.output_dir
    output_filename = state.step3_output_filename

    if not isinstance(step2_outputs, list):
        step2_outputs = []

    start_time = time.time()
    combined_output = run_step3(
        step2_outputs=step2_outputs,
        run_label=run_label,
        output_dir=output_dir,
        output_filename=output_filename,
    )

    end_time = time.time()
    logger.info(f"Step3 completed in {end_time - start_time:.2f} seconds")

    return {"step3_output": combined_output}
