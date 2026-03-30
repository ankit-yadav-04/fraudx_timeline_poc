"""Step 2: extract timeline events from cleaned chunks using batched LLM calls."""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from loguru import logger
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

load_dotenv()


# =========================
# Config
# =========================

# Global cap for concurrent LLM batch API calls across ALL files
MAX_CONCURRENT_BATCH_CALLS = 40

# How many chunks per API call
BATCH_SIZE = 5

# How many files to process concurrently
# (keeps task explosion controlled; API calls are still globally capped by MAX_CONCURRENT_BATCH_CALLS)
MAX_CONCURRENT_FILES = 8

DEFAULT_PROMPT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/extract_dates.md"
)

DEFAULT_OUTPUT_DIR = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/date_extracted"
)


# =========================
# Pydantic Schemas
# =========================

VALID_EVENT_TAGS = {
    "injury",
    "admission",
    "diagnosis",
    "surgery",
    "procedure",
    "test",
    "imaging",
    "treatment",
    "medication",
    "checkup",
    "follow_up",
    "discharge",
    "rehabilitation",
    "therapy",
    "other",
}


class Event(BaseModel):
    event_time: Optional[str] = None  # HH:MM or null
    event_tag: Literal[
        "injury",
        "admission",
        "diagnosis",
        "surgery",
        "procedure",
        "test",
        "imaging",
        "treatment",
        "medication",
        "checkup",
        "follow_up",
        "discharge",
        "rehabilitation",
        "therapy",
        "other",
    ]
    event_summary: str


class EventsByDate(BaseModel):
    date: str  # YYYY-MM-DD
    events: List[Event] = Field(default_factory=list)


class ChunkEvents(BaseModel):
    events_by_date: List[EventsByDate] = Field(default_factory=list)


class ChunkTimeline(BaseModel):
    chunk: str
    chunk_number: int
    pageNumbers: List[int] = Field(default_factory=list)
    events_by_date: List[EventsByDate] = Field(default_factory=list)


ChunkEventsAdapter = TypeAdapter(ChunkEvents)


# =========================
# IO Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_chunks(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    chunks = data.get("chunks", [])
    if not isinstance(chunks, list):
        return []
    return [c for c in chunks if isinstance(c, dict)]


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def pass1_output_path_from_pass0(pass0_path: str, output_dir: str) -> Path:
    p = Path(pass0_path)
    stem = p.stem
    if stem.endswith("_pass0"):
        stem = stem[:-6]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{stem}_pass1.json"


# =========================
# LLM / Prompt
# =========================


def build_chain(prompt_path: str):
    prompt = ChatPromptTemplate.from_template(load_prompt_from_md(prompt_path))
    llm = ChatOpenAI(
        model="gpt-5-nano",
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
        model_kwargs={"response_format": {"type": "json_object"}},
    )
    return prompt | llm


def _response_to_text(response: Any) -> str:
    # AIMessage(content=str) in common case
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict):
                txt = item.get("text")
                if isinstance(txt, str):
                    parts.append(txt)
        return "\n".join(parts)
    return str(content)


# =========================
# Parsing / Normalization
# =========================


def _coerce_unknown_tags_to_other(chunk_events_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make a best-effort normalization so unknown tags don't fail whole-key parsing.
    """
    obj = dict(chunk_events_obj)
    ebd = obj.get("events_by_date", [])
    if not isinstance(ebd, list):
        obj["events_by_date"] = []
        return obj

    normalized_days: List[Dict[str, Any]] = []
    for day in ebd:
        if not isinstance(day, dict):
            continue
        events = day.get("events", [])
        if not isinstance(events, list):
            events = []

        norm_events: List[Dict[str, Any]] = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            tag_raw = str(ev.get("event_tag", "")).strip().lower()
            if tag_raw not in VALID_EVENT_TAGS:
                tag_raw = "other"

            norm_events.append(
                {
                    "event_time": ev.get("event_time", None),
                    "event_tag": tag_raw,
                    "event_summary": str(ev.get("event_summary", "")).strip(),
                }
            )

        normalized_days.append(
            {"date": str(day.get("date", "")).strip(), "events": norm_events}
        )

    obj["events_by_date"] = normalized_days
    return obj


def _unwrap_batch_root(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Allow {"results": {...}} as alternate model output
    if set(raw.keys()) == {"results"} and isinstance(raw["results"], dict):
        return raw["results"]
    return raw


def _parse_batch_response(raw_text: str) -> Tuple[Dict[str, ChunkEvents], List[str]]:
    """
    Parse raw JSON and validate each key independently.
    Returns:
      - parsed map: key -> ChunkEvents
      - invalid keys list (failed validation)
    """
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError(f"Expected top-level JSON object, got {type(raw).__name__}")

    raw = _unwrap_batch_root(raw)

    parsed: Dict[str, ChunkEvents] = {}
    invalid_keys: List[str] = []

    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        if not isinstance(v, dict):
            invalid_keys.append(k)
            continue

        # First attempt
        try:
            parsed[k] = ChunkEventsAdapter.validate_python(v)
            continue
        except ValidationError:
            pass

        # Normalize unknown tags and try again
        try:
            v2 = _coerce_unknown_tags_to_other(v)
            parsed[k] = ChunkEventsAdapter.validate_python(v2)
            continue
        except ValidationError:
            invalid_keys.append(k)

    return parsed, invalid_keys


def _parse_single_chunk_response(raw_text: str) -> ChunkEvents:
    """
    Accepts either:
      {"events_by_date":[...]}
      {"1": {"events_by_date":[...]}}
      {"results": {"1": {"events_by_date":[...]}}}
    """
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON on retry: {e}") from e

    if isinstance(raw, dict) and "results" in raw and isinstance(raw["results"], dict):
        raw = raw["results"]

    if isinstance(raw, dict) and "1" in raw and isinstance(raw["1"], dict):
        raw = raw["1"]

    if not isinstance(raw, dict):
        raise ValueError("Single chunk retry response is not a JSON object")

    try:
        return ChunkEventsAdapter.validate_python(raw)
    except ValidationError:
        normalized = _coerce_unknown_tags_to_other(raw)
        return ChunkEventsAdapter.validate_python(normalized)


# =========================
# Batch Extraction
# =========================


def _build_chunk_timeline(chunk: Dict[str, Any], ce: ChunkEvents) -> ChunkTimeline:
    return ChunkTimeline(
        chunk=chunk.get("chunk", ""),
        chunk_number=chunk.get("chunk_number", 0),
        pageNumbers=chunk.get("pageNumbers", []),
        events_by_date=ce.events_by_date,
    )


async def _retry_single_chunk(chain, chunk: Dict[str, Any]) -> ChunkEvents:
    payload = json.dumps({"1": chunk.get("suggestedText", "")}, ensure_ascii=False)
    response = await chain.ainvoke({"batch_json": payload})
    return _parse_single_chunk_response(_response_to_text(response))


async def process_batch(chain, chunks: List[Dict[str, Any]]) -> List[ChunkTimeline]:
    if not chunks:
        return []

    key_to_chunk: Dict[str, Dict[str, Any]] = {}
    batch_payload: Dict[str, str] = {}

    for idx, chunk in enumerate(chunks, start=1):
        k = str(idx)
        key_to_chunk[k] = chunk
        batch_payload[k] = str(chunk.get("suggestedText", ""))

    batch_json = json.dumps(batch_payload, ensure_ascii=False)
    response = await chain.ainvoke({"batch_json": batch_json})
    raw_text = _response_to_text(response)

    parsed_map: Dict[str, ChunkEvents] = {}
    invalid_keys: List[str] = []

    try:
        parsed_map, invalid_keys = _parse_batch_response(raw_text)
    except ValueError as e:
        logger.error(
            f"Step2 batch parse failed: {e} — retrying all chunks individually"
        )
        invalid_keys = list(key_to_chunk.keys())

    # Keys expected from sent payload
    expected_keys = set(key_to_chunk.keys())
    returned_keys = set(parsed_map.keys())
    missing_keys = sorted(expected_keys - returned_keys)
    # include per-key validation failures
    retry_keys = sorted(set(missing_keys + invalid_keys))

    out: List[ChunkTimeline] = []

    for key, chunk in key_to_chunk.items():
        ce = parsed_map.get(key)

        if key in retry_keys or ce is None:
            chunk_number = chunk.get("chunk_number", 0)
            logger.warning(
                f"Step2 missing/invalid batch key={key} chunk_number={chunk_number} — retrying individually"
            )
            try:
                ce = await _retry_single_chunk(chain, chunk)
            except Exception as exc:
                logger.error(
                    f"Step2 individual retry failed chunk_number={chunk_number}: {exc}"
                )
                ce = ChunkEvents(events_by_date=[])

        out.append(_build_chunk_timeline(chunk, ce))

    return out


async def process_batch_with_limit(
    chain,
    chunks: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> Optional[List[ChunkTimeline]]:
    chunk_numbers = [c.get("chunk_number", 0) for c in chunks if isinstance(c, dict)]
    async with semaphore:
        logger.info(f"Step2 processing batch chunk_numbers={chunk_numbers}")
        try:
            results = await process_batch(chain, chunks)
            logger.info(f"Step2 completed batch chunk_numbers={chunk_numbers}")
            return results
        except Exception as exc:
            logger.exception(f"Step2 failed batch chunk_numbers={chunk_numbers}: {exc}")
            return None


async def process_document(
    chain,
    input_path: str,
    batch_semaphore: asyncio.Semaphore,
) -> List[ChunkTimeline]:
    chunks = load_chunks(input_path)
    if not chunks:
        logger.info(f"Step2 no chunks found in {input_path}")
        return []

    batches = [chunks[i : i + BATCH_SIZE] for i in range(0, len(chunks), BATCH_SIZE)]
    logger.info(
        f"Step2 total_chunks={len(chunks)} batches={len(batches)} batch_size={BATCH_SIZE}"
    )

    tasks = [
        process_batch_with_limit(chain, batch, batch_semaphore) for batch in batches
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    clean_results: List[ChunkTimeline] = []
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Step2 batch task exception: {r}")
            continue
        if not r:
            continue
        clean_results.extend(r)

    logger.info(f"Step2 document done total_extracted={len(clean_results)}")
    return clean_results


# =========================
# Run Entry Point
# =========================


async def process_file_with_limit(
    chain,
    input_file: str,
    file_semaphore: asyncio.Semaphore,
    batch_semaphore: asyncio.Semaphore,
    output_dir: str,
) -> Dict[str, Any]:
    async with file_semaphore:
        try:
            results = await process_document(chain, input_file, batch_semaphore)
            output_path = pass1_output_path_from_pass0(input_file, output_dir)
            save_json(output_path, [r.model_dump() for r in results])
            logger.info(f"Step2 saved {output_path}")
            return {"output": str(output_path), "failure": None}
        except Exception as exc:
            logger.exception(f"Step2 failed for file={input_file}: {exc}")
            return {
                "output": None,
                "failure": {"input_file": input_file, "error": str(exc)},
            }


async def run_step2(
    input_files: List[str],
    prompt_path: str = DEFAULT_PROMPT_PATH,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Dict[str, Any]:
    chain = build_chain(prompt_path)

    step2_outputs: List[str] = []
    step2_failures: List[Dict[str, str]] = []

    # One global semaphore for ALL API calls across all files
    batch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCH_CALLS)

    # Separate file-level concurrency control
    file_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILES)

    tasks = [
        process_file_with_limit(
            chain=chain,
            input_file=input_file,
            file_semaphore=file_semaphore,
            batch_semaphore=batch_semaphore,
            output_dir=output_dir,
        )
        for input_file in input_files
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for idx, r in enumerate(results):
        if isinstance(r, Exception):
            step2_failures.append({"input_file": input_files[idx], "error": str(r)})
            continue

        output = r.get("output")
        failure = r.get("failure")

        if output:
            step2_outputs.append(output)
        if failure:
            step2_failures.append(failure)

    return {
        "step2_outputs": step2_outputs,
        "step2_failures": step2_failures,
    }


# =========================
# LangGraph Node Wrapper
# =========================


async def step2_extract_dates_node(state: Dict[str, Any]) -> Dict[str, Any]:
    input_files = state.step1_outputs
    prompt_path = state.prompt_step2_path or DEFAULT_PROMPT_PATH

    start_time = time.time()
    result = await run_step2(
        input_files=input_files,
        prompt_path=prompt_path,
    )
    end_time = time.time()
    logger.info(f"Step2 completed in {end_time - start_time:.2f} seconds")
    return result


if __name__ == "__main__":
    # Standalone runner for Step2 only (pass0 -> pass1)

    # Edit these as needed
    input_dir = Path(
        "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons/all_xray_run_workflow2_002_10"
    )
    prompt_path = "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/extract_dates.md"

    # Pick all pass0 files in this run folder
    input_files = sorted(str(p) for p in input_dir.glob("*_pass0.json"))

    start = time.time()
    result = asyncio.run(
        run_step2(
            input_files=input_files,
            prompt_path=prompt_path,
            output_dir="/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/date_extracted",
        )
    )
    end = time.time()

    logger.info(f"Standalone Step2 completed in {end - start:.2f} seconds")
    logger.info(f"Generated pass1 files: {len(result.get('step2_outputs', []))}")
    logger.info(f"Failed files: {len(result.get('step2_failures', []))}")

    # Optional concise console summary
    print("Step2 outputs:", len(result.get("step2_outputs", [])))
    print("Step2 failures:", len(result.get("step2_failures", [])))
