"""Step 2.5: screen pass1 chunks against patient profile and produce pass2 files."""

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

load_dotenv()


# =========================
# Config
# =========================

BATCH_SIZE = 4
MAX_CONCURRENT_FILES = 8
MAX_CONCURRENT_BATCH_CALLS = 40

DEFAULT_MODEL = "gpt-4.1-nano"

DEFAULT_PROMPT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/prompt.md"
)
DEFAULT_PATIENT_PROFILE_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/patient_profile.txt"
)
DEFAULT_INPUT_DIR = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/date_extracted"
)

Decision = Literal["KEEP", "REJECT"]
VALID_DECISIONS = {"KEEP", "REJECT"}
DEFAULT_REASON = "Fallback KEEP: parse/call uncertainty."


# =========================
# IO Helpers
# =========================


def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_json_list(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected top-level list in {path}")
    return [x for x in data if isinstance(x, dict)]


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def pass2_output_path_from_pass1(pass1_path: str) -> Path:
    p = Path(pass1_path)
    stem = p.stem
    if stem.endswith("_pass1"):
        stem = stem[:-6]
    return p.with_name(f"{stem}_pass2.json")


# =========================
# LLM / Prompt
# =========================


def build_chain(prompt_path: str, model_name: str = DEFAULT_MODEL):
    prompt = ChatPromptTemplate.from_template(load_text(prompt_path))
    llm = ChatOpenAI(
        model=model_name,
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
        model_kwargs={"response_format": {"type": "json_object"}},
    )
    return prompt | llm


def _response_to_text(response: Any) -> str:
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
# Screening Helpers
# =========================


def _has_non_empty_events(chunk: Dict[str, Any]) -> bool:
    ebd = chunk.get("events_by_date", [])
    return isinstance(ebd, list) and len(ebd) > 0


def _extract_screening_event(chunk: Dict[str, Any]) -> Dict[str, str]:
    """
    Build minimal screening object from first event of first date block.
    If fields are missing, keep empty strings to avoid crashing.
    """
    date_val = ""
    event_tag = ""
    event_summary = ""

    ebd = chunk.get("events_by_date", [])
    if isinstance(ebd, list) and ebd:
        first_day = ebd[0] if isinstance(ebd[0], dict) else {}
        date_val = str(first_day.get("date", "")).strip()

        events = first_day.get("events", [])
        if isinstance(events, list) and events:
            first_event = events[0] if isinstance(events[0], dict) else {}
            event_tag = str(first_event.get("event_tag", "")).strip()
            event_summary = str(first_event.get("event_summary", "")).strip()

    return {
        "date": date_val,
        "event_tag": event_tag,
        "event_summary": event_summary,
    }


def _build_batch_payload(
    chunks: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, str]]]:
    """
    Returns:
      - key_to_chunk: key -> original chunk dict
      - payload: key -> slim screening event dict
    """
    key_to_chunk: Dict[str, Dict[str, Any]] = {}
    payload: Dict[str, Dict[str, str]] = {}

    for idx, chunk in enumerate(chunks, start=1):
        key = str(idx)
        key_to_chunk[key] = chunk
        payload[key] = _extract_screening_event(chunk)

    return key_to_chunk, payload


def _parse_decision_map(raw_text: str) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    """
    Parse model JSON output in new shape:
      {"1":{"decision":"KEEP","reason":"..."}, "2":{"decision":"REJECT","reason":"..."}}

    Returns:
      - parsed map: key -> {"decision": "...", "reason": "..."}
      - invalid_keys list
    """
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from model: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError(f"Expected JSON object, got {type(raw).__name__}")

    # Optional wrapper handling
    if set(raw.keys()) == {"results"} and isinstance(raw["results"], dict):
        raw = raw["results"]

    parsed: Dict[str, Dict[str, str]] = {}
    invalid_keys: List[str] = []

    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        if not isinstance(v, dict):
            invalid_keys.append(k)
            continue

        decision = str(v.get("decision", "")).strip().upper()
        reason = str(v.get("reason", "")).strip()

        if decision in VALID_DECISIONS:
            if not reason:
                reason = f"{decision}: reason not provided by model."
            parsed[k] = {"decision": decision, "reason": reason}
        else:
            invalid_keys.append(k)

    return parsed, invalid_keys


def _apply_decisions(
    key_to_chunk: Dict[str, Dict[str, Any]],
    parsed_decisions: Dict[str, Dict[str, str]],
    invalid_keys: List[str],
) -> List[Dict[str, Any]]:
    """
    Attach decision + reason. Safe fallback KEEP for missing/invalid keys.
    """
    expected_keys = set(key_to_chunk.keys())
    returned_keys = set(parsed_decisions.keys())
    missing_keys = sorted(expected_keys - returned_keys)
    fallback_keys = sorted(set(missing_keys + invalid_keys))

    out: List[Dict[str, Any]] = []

    for key, chunk in key_to_chunk.items():
        info = parsed_decisions.get(key)
        decision: Decision = "KEEP"
        reason = DEFAULT_REASON

        if info:
            parsed_decision = info.get("decision", "KEEP").upper()
            if parsed_decision in VALID_DECISIONS:
                decision = parsed_decision  # type: ignore[assignment]
            reason = (
                info.get("reason", "") or f"{decision}: reason not provided by model."
            )

        if key in fallback_keys:
            decision = "KEEP"
            reason = f"Fallback KEEP: missing/invalid response for key={key}."

        chunk_out = dict(chunk)
        chunk_out["decision"] = decision
        chunk_out["reason"] = reason
        out.append(chunk_out)

    return out


# =========================
# Batch Processing
# =========================


async def process_batch(
    chain,
    patient_profile: str,
    chunks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not chunks:
        return []

    key_to_chunk, payload = _build_batch_payload(chunks)
    batch_json = json.dumps(payload, ensure_ascii=False)

    try:
        response = await chain.ainvoke(
            {
                "patient_profile": patient_profile,
                "batch_json": batch_json,
            }
        )
        raw_text = _response_to_text(response)
        parsed, invalid_keys = _parse_decision_map(raw_text)
        return _apply_decisions(key_to_chunk, parsed, invalid_keys)

    except Exception as exc:
        # Full call failure or full parse failure => default ALL KEEP
        logger.error(f"Step2.5 batch failed; defaulting KEEP for all keys: {exc}")
        fallback: List[Dict[str, Any]] = []
        for chunk in chunks:
            c = dict(chunk)
            c["decision"] = "KEEP"
            c["reason"] = f"Fallback KEEP: batch failure ({str(exc)[:160]})"
            fallback.append(c)
        return fallback


async def process_batch_with_limit(
    chain,
    patient_profile: str,
    chunks: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> Optional[List[Dict[str, Any]]]:
    chunk_numbers = [c.get("chunk_number", 0) for c in chunks if isinstance(c, dict)]
    async with semaphore:
        logger.info(f"Step2.5 processing batch chunk_numbers={chunk_numbers}")
        try:
            result = await process_batch(chain, patient_profile, chunks)
            logger.info(f"Step2.5 completed batch chunk_numbers={chunk_numbers}")
            return result
        except Exception as exc:
            logger.exception(
                f"Step2.5 failed batch chunk_numbers={chunk_numbers}: {exc}"
            )
            # hard fallback
            fallback: List[Dict[str, Any]] = []
            for chunk in chunks:
                c = dict(chunk)
                c["decision"] = "KEEP"
                c["reason"] = f"Fallback KEEP: batch exception ({str(exc)[:160]})"
                fallback.append(c)
            return fallback


# =========================
# File Processing
# =========================


async def process_file(
    chain,
    patient_profile: str,
    input_file: str,
    batch_semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    """
    Load one pass1 file, drop empty chunks, screen non-empty chunks, write pass2 file.
    """
    chunks = load_json_list(input_file)

    non_empty_chunks = [c for c in chunks if _has_non_empty_events(c)]
    empty_dropped = len(chunks) - len(non_empty_chunks)

    logger.info(
        f"Step2.5 file start={input_file} total_chunks={len(chunks)} non_empty={len(non_empty_chunks)} dropped_empty={empty_dropped}"
    )

    if not non_empty_chunks:
        output_path = pass2_output_path_from_pass1(input_file)
        save_json(output_path, [])
        logger.info(f"Step2.5 saved {output_path} keep=0 reject=0")
        return {
            "output": str(output_path),
            "failure": None,
            "keep_count": 0,
            "reject_count": 0,
            "dropped_empty": empty_dropped,
        }

    batches = [
        non_empty_chunks[i : i + BATCH_SIZE]
        for i in range(0, len(non_empty_chunks), BATCH_SIZE)
    ]

    tasks = [
        process_batch_with_limit(chain, patient_profile, batch, batch_semaphore)
        for batch in batches
    ]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    screened_chunks: List[Dict[str, Any]] = []
    for idx, r in enumerate(batch_results):
        if isinstance(r, Exception):
            logger.error(f"Step2.5 batch task exception index={idx}: {r}")
            # conservative fallback for this batch
            for chunk in batches[idx]:
                c = dict(chunk)
                c["decision"] = "KEEP"
                c["reason"] = "Fallback KEEP: batch task exception."

                screened_chunks.append(c)
            continue
        if not r:
            # conservative fallback for this batch
            for chunk in batches[idx]:
                c = dict(chunk)
                c["decision"] = "KEEP"
                c["reason"] = "Fallback KEEP: empty batch result."

                screened_chunks.append(c)
            continue
        screened_chunks.extend(r)

    # stable ordering by chunk_number
    screened_chunks.sort(key=lambda x: int(x.get("chunk_number", 0)))

    keep_count = sum(1 for c in screened_chunks if c.get("decision") == "KEEP")
    reject_count = sum(1 for c in screened_chunks if c.get("decision") == "REJECT")

    output_path = pass2_output_path_from_pass1(input_file)
    save_json(output_path, screened_chunks)

    logger.info(
        f"Step2.5 saved {output_path} keep={keep_count} reject={reject_count} dropped_empty={empty_dropped}"
    )

    return {
        "output": str(output_path),
        "failure": None,
        "keep_count": keep_count,
        "reject_count": reject_count,
        "dropped_empty": empty_dropped,
    }


async def process_file_with_limit(
    chain,
    patient_profile: str,
    input_file: str,
    file_semaphore: asyncio.Semaphore,
    batch_semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with file_semaphore:
        try:
            return await process_file(
                chain, patient_profile, input_file, batch_semaphore
            )
        except Exception as exc:
            logger.exception(f"Step2.5 failed file={input_file}: {exc}")
            return {
                "output": None,
                "failure": {"input_file": input_file, "error": str(exc)},
                "keep_count": 0,
                "reject_count": 0,
                "dropped_empty": 0,
            }


# =========================
# Run Entry
# =========================


async def run_step2_5(
    input_files: List[str],
    prompt_path: str = DEFAULT_PROMPT_PATH,
    patient_profile_path: str = DEFAULT_PATIENT_PROFILE_PATH,
) -> Dict[str, Any]:
    """
    Run screening on pass1 files and write pass2 files alongside them.
    """
    patient_profile = load_text(patient_profile_path)
    chain = build_chain(prompt_path, model_name=DEFAULT_MODEL)

    file_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILES)
    batch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCH_CALLS)

    tasks = [
        process_file_with_limit(
            chain=chain,
            patient_profile=patient_profile,
            input_file=input_file,
            file_semaphore=file_semaphore,
            batch_semaphore=batch_semaphore,
        )
        for input_file in input_files
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    outputs: List[str] = []
    failures: List[Dict[str, str]] = []
    total_keep = 0
    total_reject = 0
    total_dropped_empty = 0

    for idx, r in enumerate(results):
        if isinstance(r, Exception):
            failures.append({"input_file": input_files[idx], "error": str(r)})
            continue

        out = r.get("output")
        failure = r.get("failure")
        if out:
            outputs.append(out)
        if failure:
            failures.append(failure)

        total_keep += int(r.get("keep_count", 0))
        total_reject += int(r.get("reject_count", 0))
        total_dropped_empty += int(r.get("dropped_empty", 0))

    return {
        "step2_5_outputs": outputs,
        "step2_5_failures": failures,
        "total_keep": total_keep,
        "total_reject": total_reject,
        "total_dropped_empty": total_dropped_empty,
    }


if __name__ == "__main__":
    # Standalone runner
    input_dir = Path(DEFAULT_INPUT_DIR)
    input_files = sorted(str(p) for p in input_dir.glob("*_pass1.json"))

    start = time.time()
    result = asyncio.run(
        run_step2_5(
            input_files=input_files,
            prompt_path=DEFAULT_PROMPT_PATH,
            patient_profile_path=DEFAULT_PATIENT_PROFILE_PATH,
        )
    )
    end = time.time()

    logger.info(f"Step2.5 completed in {end - start:.2f} seconds")
    logger.info(f"Outputs: {len(result.get('step2_5_outputs', []))}")
    logger.info(f"Failures: {len(result.get('step2_5_failures', []))}")
    logger.info(
        f"Totals => KEEP={result.get('total_keep', 0)} "
        f"REJECT={result.get('total_reject', 0)} "
        f"DROPPED_EMPTY={result.get('total_dropped_empty', 0)}"
    )

    print("step2_5_outputs:", len(result.get("step2_5_outputs", [])))
    print("step2_5_failures:", len(result.get("step2_5_failures", [])))
