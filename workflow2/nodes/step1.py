"""Step 1: clean raw chunks and persist pass0 JSON artifacts."""

import json
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger
import time


DEFAULT_OUTPUT_DIR = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons"


def transform_chunks(input_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Keep selected chunk keys and add sequential chunk_number.
    Returns: {"chunks": [...]}
    """
    required_keys = ["boundingBoxes", "chunk", "pageNumbers", "suggestedText"]
    source_chunks = input_data.get("chunks", [])

    transformed: List[Dict[str, Any]] = []

    if not isinstance(source_chunks, list):
        return {"chunks": transformed}

    for ch in source_chunks:
        if not isinstance(ch, dict):
            continue

        new_chunk = {k: ch.get(k) for k in required_keys}
        new_chunk["chunk_number"] = len(transformed) + 1
        transformed.append(new_chunk)

    return {"chunks": transformed}


def _safe_stem(file_path: str) -> str:
    """Return a safe base filename for output artifact naming."""
    return Path(file_path).stem or "input"


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return data


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def process_one_file(input_file: str, output_file: str) -> str:
    """Process one raw input file and write cleaned pass0 output."""
    input_path = Path(input_file)
    output_path = Path(output_file)

    input_data = _load_json(input_path)
    cleaned = transform_chunks(input_data)
    _save_json(output_path, cleaned)

    return str(output_path)


def run_step1(
    input_files: List[str],
    run_label: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> List[str]:
    """
    Process all input files and write pass0 outputs under:
    {output_dir}/{run_label}/
    """
    if not input_files:
        return []

    run_dir = Path(output_dir) / run_label
    output_paths: List[str] = []

    for input_file in input_files:
        base = _safe_stem(input_file)
        output_file = run_dir / f"{base}_pass0.json"
        written_path = process_one_file(input_file, str(output_file))
        output_paths.append(written_path)

    return output_paths


async def step1_clean_chunks_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    LangGraph node-compatible wrapper.

    Expected state keys:
    - input_raw_files: List[str]
    - run_label: str (optional, defaults to 'run')
    - output_dir: str (optional)
    """
    input_files = state.input_raw_files
    run_label = state.run_label
    output_dir = state.output_dir

    start_time = time.time()
    step1_outputs = run_step1(
        input_files=input_files,
        run_label=run_label,
        output_dir=output_dir,
    )

    end_time = time.time()
    logger.info(f"Step1 completed in {end_time - start_time:.2f} seconds")

    return {"step1_outputs": step1_outputs}
