"""Workflow graph nodes that replicate pass1 -> pass5 pipeline."""

from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Literal, Optional, cast

import time
from loguru import logger
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from workflow.graph.state import FraudTimelineWorkflowState

load_dotenv()


PIPELINE_ROOT = Path("/home/ankit/smartsense_code/fraudx_timeline_poc/pipeline")

PASS1_PROMPT_PATH = PIPELINE_ROOT / "pass1" / "extract_dates.md"
PASS2_PROMPT_PATH = PIPELINE_ROOT / "pass2" / "merging_prompt.md"
PASS2_5_PROMPT_PATH = PIPELINE_ROOT / "pass2_5" / "clean_doc_prompt.md"
PASS3_PROMPT_PATH = PIPELINE_ROOT / "pass3" / "prompt.md"
PASS5_PROMPT_PATH = PIPELINE_ROOT / "pass5" / "prompt.md"

PASS1_BATCH_CONCURRENCY = 5
PASS2_BATCH_SIZE = 10
PASS2_5_BATCH_SIZE_DATES = 10
PASS3_LARGE_DATE_THRESHOLD = 10
MAX_PARALLEL_LLM_CALLS = 5

PASS3_TAG_ORDER = [
    "admission",
    "injury",
    "diagnosis",
    "imaging",
    "procedure",
    "surgery",
    "medication",
    "checkup",
    "test",
    "discharge",
    "other",
]
PASS3_TAG_RANK = {tag: idx for idx, tag in enumerate(PASS3_TAG_ORDER)}


# ----------------------------
# Shared helpers
# ----------------------------


def _ensure_run_dir(state: FraudTimelineWorkflowState) -> Path:
    run_dir = Path(state.output_dir) / state.run_label
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _dump_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _safe_extract_json_object(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = [ln for ln in cleaned.splitlines() if not ln.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM output does not contain a JSON object.")
    parsed = json.loads(cleaned[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Parsed output is not a JSON object.")
    return parsed


def _safe_extract_json_array(text: str) -> list[dict[str, Any]]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = [ln for ln in cleaned.splitlines() if not ln.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM output does not contain a JSON array.")
    parsed = json.loads(cleaned[start : end + 1])
    if not isinstance(parsed, list):
        raise ValueError("Parsed output is not a JSON array.")
    return parsed


def _parse_date_safe(item: Dict[str, Any]) -> tuple[int, datetime, str]:
    value = item.get("event_date") or item.get("date")
    if not value:
        return (1, datetime.max, "")
    try:
        return (0, datetime.strptime(str(value), "%Y-%m-%d"), str(value))
    except ValueError:
        return (1, datetime.max, str(value))


def _chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _get_llm(temp: float = 0.0) -> ChatOpenAI:
    return ChatOpenAI(
        model="gpt-4.1-mini",
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=temp,
    )


# ----------------------------
# Pass1 schemas + helpers
# ----------------------------


class Pass1Event(BaseModel):
    """
    Represents a single event extracted from a document chunk.
    """
    event_time: Optional[str] = None
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


class Pass1EventsByDate(BaseModel):
    """
    Represents a single date with a list of events extracted from a document chunk.
    """
    date: str
    events: list[Pass1Event] = Field(default_factory=list)


class Pass1ChunkEvents(BaseModel):
    """
    Represents a list of dates with a list of events extracted from a document chunk.
    """
    events_by_date: list[Pass1EventsByDate] = Field(default_factory=list)


async def _pass1_process_single_chunk(
    chunk: Dict[str, Any],
    chain: Any,
    semaphore: asyncio.Semaphore,
    default_chunk_number: int = 0,
) -> Dict[str, Any]:
    """
    Processes a single document chunk to extract events and their dates.
    """
    async with semaphore:
        response = cast(Pass1ChunkEvents, await chain.ainvoke({"chunk_text": chunk["suggestedText"]}))
        return {
            "chunk": chunk.get("chunk", ""),
            "chunk_number": chunk.get("chunk_number", default_chunk_number),
            "pageNumbers": chunk.get("pageNumbers", []),
            "events_by_date": [d.model_dump() for d in response.events_by_date],
        }


async def pass1_extract_dates_node(state: FraudTimelineWorkflowState) -> dict:
    """
    Extracts events and their dates from a list of document chunks.
    """

    logger.info(f"Pass1 extract dates node started for {state.input_chunk_files}")
    start_time = time.time()
    run_dir = _ensure_run_dir(state)
    prompt = ChatPromptTemplate.from_template(_load_text(PASS1_PROMPT_PATH))
    llm = _get_llm(0.1).with_structured_output(Pass1ChunkEvents)
    chain = prompt | llm
    semaphore = asyncio.Semaphore(PASS1_BATCH_CONCURRENCY)

    outputs: list[str] = []
    for input_file in state.input_chunk_files:
        data = _load_json(Path(input_file))
        chunks = data.get("chunks", []) if isinstance(data, dict) else []
        tasks = [
            _pass1_process_single_chunk(chunk, chain, semaphore, default_chunk_number=idx)
            for idx, chunk in enumerate(chunks, start=1)
            if isinstance(chunk, dict) and "suggestedText" in chunk
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        clean_results = [r for r in results if isinstance(r, dict)]

        output_path = run_dir / f"{Path(input_file).stem}_pass1.json"
        _dump_json(output_path, clean_results)
        outputs.append(str(output_path))

    end_time = time.time()
    logger.info(f"Pass1 extract dates node completed in {end_time - start_time} seconds")
    return {"pass1_outputs": outputs}


# ----------------------------
# Pass2 helpers + node
# ----------------------------


def _pass2_normalize_time(value: Optional[str]) -> str:
    """
    Normalize time to string format expected by Pass2 prompt.
    """
    return "null" if value in (None, "", "null", "None") else str(value).strip()


def _pass2_flatten_chunks_to_lines(chunks: list[dict[str, Any]]) -> list[str]:
    """
    Input shape:
      chunk -> events_by_date[] -> events[]
    Output lines:
      DATE | TIME | TAG | SUMMARY
    """
    lines: list[str] = []

    for chunk_obj in chunks:
        for by_date in chunk_obj.get("events_by_date", []):
            date = by_date.get("date")
            if not date:
                continue

            for ev in by_date.get("events", []):
                event_time = _pass2_normalize_time(ev.get("event_time"))
                tag = (ev.get("event_tag") or "").strip()
                summary = (ev.get("event_summary") or "").strip()

                if not tag or not summary:
                    continue

                lines.append(f"{date} | {event_time} | {tag} | {summary}")

    return lines


def _pass2_parse_lines(raw_text: str) -> list[dict[str, Optional[str]]]:
    """
    Parse LLM output lines in format:
      DATE | TIME | TAG | SUMMARY
    """
    parsed: list[dict[str, Optional[str]]] = []

    for line in raw_text.splitlines():
        clean = line.strip()
        if not clean:
            continue

        # Split at first 3 pipes; summary may itself contain pipes.
        parts = [p.strip() for p in clean.split("|", 3)]
        if len(parts) != 4:
            continue

        date, time_val, tag, summary = parts
        if not date or not tag or not summary:
            continue

        event_time: Optional[str]
        if time_val.lower() in {"null", "none", ""}:
            event_time = None
        else:
            event_time = time_val

        parsed.append(
            {
                "date": date,
                "event_time": event_time,
                "event_tag": tag,
                "event_summary": summary,
            }
        )

    return parsed


def _pass2_group_events_by_date(
    events: list[dict[str, Optional[str]]],
) -> list[dict[str, Any]]:
    """
    Group events by date and sort events within date by time asc.
    Null-time events are kept after timed events.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for e in events:
        date = str(e["date"])
        grouped.setdefault(date, []).append(
            {
                "event_time": e["event_time"],
                "event_tag": e["event_tag"],
                "event_summary": e["event_summary"],
            }
        )

    events_by_date: list[dict[str, Any]] = []
    for date in sorted(grouped.keys()):
        day_events = grouped[date]
        day_events.sort(
            key=lambda ev: (
                ev["event_time"] is None,  # False first (timed), True last (null)
                ev["event_time"] or "99:99",
            )
        )
        events_by_date.append({"date": date, "events": day_events})

    return events_by_date


async def _pass2_merge_event_lines(
    event_lines: list[str], llm: ChatOpenAI, prompt_template: str
) -> list[str]:
    """
    Send pipe-delimited lines to LLM and return cleaned pipe-delimited lines.
    """
    if not event_lines:
        return []

    input_block = "\n".join(event_lines)
    prompt_text = prompt_template.replace("{{EVENTS}}", input_block)

    response = await llm.ainvoke(prompt_text)
    content = response.content if isinstance(response.content, str) else str(response.content)

    # Keep non-empty lines only.
    return [line.strip() for line in content.splitlines() if line.strip()]


async def _pass2_merge_pair(
    lines_a: list[str],
    lines_b: list[str],
    semaphore: asyncio.Semaphore,
    llm: ChatOpenAI,
    prompt_template: str,
) -> list[str]:
    """
    Merge two line-lists under concurrency guard.
    """
    async with semaphore:
        return await _pass2_merge_event_lines(lines_a + lines_b, llm, prompt_template)


async def _pass2_merge_level(
    nodes: list[list[str]],
    semaphore: asyncio.Semaphore,
    llm: ChatOpenAI,
    prompt_template: str,
) -> list[list[str]]:
    """
    Merge one binary-tree level.
    """
    tasks: list[asyncio.Task] = []
    next_level: list[list[str]] = []

    i = 0
    while i < len(nodes):
        if i + 1 >= len(nodes):
            next_level.append(nodes[i])  # odd node pass-through
            break

        tasks.append(
            asyncio.create_task(
                _pass2_merge_pair(
                    nodes[i],
                    nodes[i + 1],
                    semaphore=semaphore,
                    llm=llm,
                    prompt_template=prompt_template,
                )
            )
        )
        i += 2

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                raise r
            next_level.append(r)

    return next_level


async def _pass2_binary_merge_all(
    nodes: list[list[str]], llm: ChatOpenAI, prompt_template: str
) -> list[str]:
    """
    Binary-tree merge across all batches (global dedup).
    """
    if not nodes:
        return []

    semaphore = asyncio.Semaphore(MAX_PARALLEL_LLM_CALLS)
    level = 1

    while len(nodes) > 1:
        logger.info(f"Pass2 merge level {level}: {len(nodes)} node(s)")
        nodes = await _pass2_merge_level(nodes, semaphore, llm, prompt_template)
        level += 1

    return nodes[0]


async def pass2_document_timeline_node(state: FraudTimelineWorkflowState) -> dict:
    """
    Build pass2 document timelines from pass1 outputs.
    One pass2 output file is generated per pass1 input file.
    """
    logger.info(f"Pass2 document timeline node started for {state.pass1_outputs}")
    start_time = time.time()

    run_dir = _ensure_run_dir(state)
    prompt_template = _load_text(PASS2_PROMPT_PATH)
    llm = _get_llm(0.0)

    outputs: list[str] = []
    for pass1_file in state.pass1_outputs:
        chunks = _load_json(Path(pass1_file))
        batches = _chunked(chunks, PASS2_BATCH_SIZE)

        # Step 1: per-batch flatten + dedup
        batch_nodes: list[list[str]] = []
        for idx, batch in enumerate(batches, start=1):
            flat_lines = _pass2_flatten_chunks_to_lines(batch)
            logger.info(f"Pass2 {Path(pass1_file).name} batch {idx}: flattened {len(flat_lines)} lines")

            cleaned_lines = (
                await _pass2_merge_event_lines(flat_lines, llm, prompt_template)
                if flat_lines
                else []
            )
            logger.info(f"Pass2 {Path(pass1_file).name} batch {idx}: cleaned {len(cleaned_lines)} lines")
            batch_nodes.append(cleaned_lines)

        # Step 2: binary merge across batches
        final_lines = await _pass2_binary_merge_all(batch_nodes, llm, prompt_template)

        # Step 3: parse + regroup to JSON
        parsed_events = _pass2_parse_lines("\n".join(final_lines))
        output_obj = {"events_by_date": _pass2_group_events_by_date(parsed_events)}

        output_path = run_dir / f"{Path(pass1_file).stem.replace('_pass1', '')}_pass2.json"
        _dump_json(output_path, output_obj)
        outputs.append(str(output_path))

    end_time = time.time()
    logger.info(f"Pass2 document timeline node completed in {end_time - start_time} seconds")
    return {"pass2_outputs": outputs}


# ----------------------------
# Pass2.5 helpers + node
# ----------------------------


# ----------------------------
# Pass2.5 helpers + node
# ----------------------------


async def _pass2_5_clean_once(
    batch_input_json: dict[str, Any],
    llm: ChatOpenAI,
    prompt_template: str,
) -> dict[str, Any]:
    """
    Clean a single batch using Pass2.5 prompt.
    """
    input_block = json.dumps(batch_input_json, ensure_ascii=False, indent=2)
    prompt_text = prompt_template.replace("{PASS_2_JSON}", input_block)

    response = await llm.ainvoke(prompt_text)
    content = response.content if isinstance(response.content, str) else str(response.content)

    return _safe_extract_json_object(content)


def _pass2_5_normalize_for_prompt(batch_dates: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Keep exactly the structure expected by clean_doc_prompt.md input contract:
    {
      "events_by_date": [
        {"date": "...", "events": [{"event_time": ..., "event_tag": ..., "event_summary": ...}]}
      ]
    }
    """
    normalized: list[dict[str, Any]] = []

    for day in batch_dates:
        date_val = day.get("date")
        if not date_val:
            continue

        day_events: list[dict[str, Any]] = []
        for ev in day.get("events", []):
            tag = ev.get("event_tag")
            summary = ev.get("event_summary")
            if not tag or not summary:
                continue

            day_events.append(
                {
                    "event_time": ev.get("event_time"),
                    "event_tag": tag,
                    "event_summary": summary,
                }
            )

        normalized.append({"date": date_val, "events": day_events})

    return {"events_by_date": normalized}


def _pass2_5_to_pass2_shape(clean_json: dict[str, Any]) -> dict[str, Any]:
    """
    Convert clean output back to pass2-like structure so we can re-merge pairs
    with the same prompt during binary merge.
    """
    converted_days: list[dict[str, Any]] = []

    for day in clean_json.get("events_by_date", []):
        date_val = day.get("event_date")
        events_out: list[dict[str, Any]] = []

        for ev in day.get("events", []):
            tags = ev.get("event_tags", [])
            details = ev.get("event_details", [])

            # Choose stable primary tag for re-input; keep additional tags in summary text.
            primary_tag = tags[0] if tags else "other"
            summary = "; ".join(details).strip() if details else "No details provided"

            if len(tags) > 1:
                summary = f"[tags: {', '.join(tags)}] {summary}"

            events_out.append(
                {
                    "event_time": None,  # pass2_5 output has no event_time
                    "event_tag": primary_tag,
                    "event_summary": summary,
                }
            )

        converted_days.append({"date": date_val, "events": events_out})

    return {"events_by_date": converted_days}


async def _pass2_5_merge_pair(
    node_a: dict[str, Any],
    node_b: dict[str, Any],
    semaphore: asyncio.Semaphore,
    llm: ChatOpenAI,
    prompt_template: str,
) -> dict[str, Any]:
    """
    Merge two clean nodes by converting to pass2-like shape and re-cleaning once.
    """
    combined_pass2_like = {"events_by_date": []}
    combined_pass2_like["events_by_date"].extend(
        _pass2_5_to_pass2_shape(node_a).get("events_by_date", [])
    )
    combined_pass2_like["events_by_date"].extend(
        _pass2_5_to_pass2_shape(node_b).get("events_by_date", [])
    )

    # Keep chronological order for deterministic behavior.
    combined_pass2_like["events_by_date"].sort(key=lambda d: d.get("date", ""))

    async with semaphore:
        return await _pass2_5_clean_once(combined_pass2_like, llm, prompt_template)


async def _pass2_5_merge_level(
    nodes: list[dict[str, Any]],
    semaphore: asyncio.Semaphore,
    llm: ChatOpenAI,
    prompt_template: str,
) -> list[dict[str, Any]]:
    """
    Merge one level of nodes in a binary tree.
    """
    tasks: list[asyncio.Task] = []
    next_level: list[dict[str, Any]] = []

    i = 0
    while i < len(nodes):
        if i + 1 >= len(nodes):
            next_level.append(nodes[i])  # odd node pass-through
            break

        tasks.append(
            asyncio.create_task(
                _pass2_5_merge_pair(
                    nodes[i],
                    nodes[i + 1],
                    semaphore=semaphore,
                    llm=llm,
                    prompt_template=prompt_template,
                )
            )
        )
        i += 2

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                raise r
            next_level.append(r)

    return next_level


async def _pass2_5_binary_merge(
    nodes: list[dict[str, Any]],
    llm: ChatOpenAI,
    prompt_template: str,
) -> dict[str, Any]:
    """
    Binary-tree merge across all batch outputs to deduplicate globally.
    """
    if not nodes:
        return {"events_by_date": []}

    semaphore = asyncio.Semaphore(MAX_PARALLEL_LLM_CALLS)
    level = 1

    while len(nodes) > 1:
        logger.info(f"Pass2.5 merge level {level}: {len(nodes)} node(s)")
        nodes = await _pass2_5_merge_level(nodes, semaphore, llm, prompt_template)
        level += 1

    return nodes[0]


async def pass2_5_clean_doc_node(state: FraudTimelineWorkflowState) -> dict:
    """
    Build pass2.5 cleaned timelines from pass2 outputs.
    One pass2.5 output file is generated per pass2 input file.
    """
    logger.info(f"Pass2.5 clean document node started for {state.pass2_outputs}")
    start_time = time.time()

    run_dir = _ensure_run_dir(state)
    prompt_template = _load_text(PASS2_5_PROMPT_PATH)
    llm = _get_llm(0.0)

    outputs: list[str] = []
    for pass2_file in state.pass2_outputs:
        pass2_json = _load_json(Path(pass2_file))
        by_date = pass2_json.get("events_by_date", [])
        by_date_sorted = sorted(by_date, key=lambda d: d.get("date", ""))
        date_batches = _chunked(by_date_sorted, PASS2_5_BATCH_SIZE_DATES)

        # Step 1: clean each batch independently
        nodes: list[dict[str, Any]] = []
        for idx, batch in enumerate(date_batches, start=1):
            normalized = _pass2_5_normalize_for_prompt(batch)
            event_count = sum(len(d.get("events", [])) for d in normalized["events_by_date"])
            logger.info(
                f"Pass2.5 {Path(pass2_file).name} batch {idx}: "
                f"{len(normalized['events_by_date'])} date(s), {event_count} event(s)"
            )

            cleaned = await _pass2_5_clean_once(normalized, llm, prompt_template)
            nodes.append(cleaned)

        # Step 2: merge across batches (global dedup)
        final_clean = await _pass2_5_binary_merge(nodes, llm, prompt_template)

        # Final stable sort by event_date
        final_clean["events_by_date"] = sorted(
            final_clean.get("events_by_date", []),
            key=lambda d: d.get("event_date", ""),
        )

        output_path = run_dir / f"{Path(pass2_file).stem.replace('_pass2', '')}_pass2_5.json"
        _dump_json(output_path, final_clean)
        outputs.append(str(output_path))

    end_time = time.time()
    logger.info(f"Pass2.5 clean document node completed in {end_time - start_time} seconds")
    return {"pass2_5_outputs": outputs}


# ----------------------------
# Pass2.51 merge node
# ----------------------------


async def merge_pass2_5_node(state: FraudTimelineWorkflowState) -> dict:
    run_dir = _ensure_run_dir(state)
    merged_events_by_date: list[dict[str, Any]] = []
    for file_path in state.pass2_5_outputs:
        data = _load_json(Path(file_path))
        events = data.get("events_by_date", [])
        if isinstance(events, list):
            merged_events_by_date.extend(events)

    merged_events_by_date.sort(key=_parse_date_safe)
    output = {"events_by_date": merged_events_by_date}
    output_path = run_dir / "merged_pass2_5.json"
    _dump_json(output_path, output)
    return {"merged_pass2_5_output": str(output_path)}


# ----------------------------
# Pass3 node
# ----------------------------


class _Pass3Flag(BaseModel):
    """
    Flag model.
    """
    flag_type: str
    severity: Literal["high", "medium", "low"]
    detail: str
    insurance_relevance: str


class _Pass3Event(BaseModel):
    """
    Event model.
    """
    event_tags: list[str] = Field(default_factory=list)
    event_details: list[str] = Field(default_factory=list)
    event_flags: list[_Pass3Flag] = Field(default_factory=list)


def _pass3_event_rank(event: dict[str, Any]) -> int:
    """
    Rank the event.
    """
    tags = event.get("event_tags", [])
    if not tags:
        return len(PASS3_TAG_ORDER) + 1
    return min(PASS3_TAG_RANK.get(str(tag), len(PASS3_TAG_ORDER) + 1) for tag in tags)


async def _pass3_merge_once(
    events_a: list[dict[str, Any]],
    events_b: list[dict[str, Any]],
    llm: ChatOpenAI,
    prompt_template: str,
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """
    Merge once.
    """
    payload = {"events_a": events_a, "events_b": events_b}
    prompt_text = prompt_template.replace("{EVENTS_JSON}", json.dumps(payload, ensure_ascii=False, indent=2))
    async with semaphore:
        response = await llm.ainvoke(prompt_text)
    content = response.content if isinstance(response.content, str) else str(response.content)
    return _safe_extract_json_array(content)


async def _pass3_merge_large_date(
    events: list[dict[str, Any]],
    llm: ChatOpenAI,
    prompt_template: str,
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """
    Merge large date.
    """
    nodes: list[list[dict[str, Any]]] = [[e] for e in events]
    while len(nodes) > 1:
        tasks: list[asyncio.Task] = []
        next_nodes: list[list[dict[str, Any]]] = []
        i = 0
        while i < len(nodes):
            if i + 1 >= len(nodes):
                next_nodes.append(nodes[i])
                break
            tasks.append(asyncio.create_task(_pass3_merge_once(nodes[i], nodes[i + 1], llm, prompt_template, semaphore)))
            i += 2
        if tasks:
            next_nodes.extend(await asyncio.gather(*tasks))
        nodes = next_nodes
    return nodes[0] if nodes else []


async def pass3_complete_timeline_node(state: FraudTimelineWorkflowState) -> dict:
    """
    Complete the timeline node.
    """
    logger.info(f"Pass3 complete timeline node started for {state.merged_pass2_5_output}")
    start_time = time.time()
    if not state.merged_pass2_5_output:
        raise ValueError("merged_pass2_5_output is missing.")

    run_dir = _ensure_run_dir(state)
    prompt_template = _load_text(PASS3_PROMPT_PATH)
    llm = _get_llm(0.0)
    semaphore = asyncio.Semaphore(MAX_PARALLEL_LLM_CALLS)

    merged = _load_json(Path(state.merged_pass2_5_output))
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in merged.get("events_by_date", []):
        event_date = entry.get("event_date")
        for event in entry.get("events", []):
            if event_date and isinstance(event, dict):
                grouped[event_date].append(event)

    final_events_by_date = []
    for date_key in sorted(grouped.keys(), key=lambda d: datetime.strptime(d, "%Y-%m-%d")):
        events = sorted(grouped[date_key], key=_pass3_event_rank)
        if len(events) == 1:
            final_events = [_Pass3Event.model_validate(events[0]).model_dump()]
        elif len(events) > PASS3_LARGE_DATE_THRESHOLD:
            merged_events = await _pass3_merge_large_date(events, llm, prompt_template, semaphore)
            final_events = [_Pass3Event.model_validate(e).model_dump() for e in merged_events]
        else:
            mid = len(events) // 2
            merged_events = await _pass3_merge_once(events[:mid], events[mid:], llm, prompt_template, semaphore)
            final_events = [_Pass3Event.model_validate(e).model_dump() for e in merged_events]
        final_events_by_date.append({"event_date": date_key, "events": final_events})

    end_time = time.time()
    logger.info(f"Pass3 complete timeline node completed in {end_time - start_time} seconds")
    output = {"events_by_date": final_events_by_date}
    output_path = run_dir / "pass3_output.json"
    _dump_json(output_path, output)
    return {"pass3_output": str(output_path)}


# ----------------------------
# Pass4 node
# ----------------------------


def _pass4_format_flags(flags: list[dict[str, Any]]) -> str:
    """
    Format the flags.
    """
    if not flags:
        return "-"
    parts = []
    for item in flags:
        parts.append(
            f"{item.get('flag_type', '')}[{item.get('severity', '')}]:{item.get('detail', '')}"
        )
    return " || ".join(parts)


def _pass4_format_event(event: dict[str, Any]) -> str:
    """
    Format the event.
    """
    tags = "+".join(event.get("event_tags", []))
    details = "; ".join(event.get("event_details", []))
    flags = _pass4_format_flags(event.get("event_flags", []))
    return f"{tags}|{details}|{flags}"


async def pass4_compress_node(state: FraudTimelineWorkflowState) -> dict:
    """
    Compress the node.
    """
    logger.info(f"Pass4 compress node started for {state.pass3_output}")
    start_time = time.time()
    if not state.pass3_output:
        raise ValueError("pass3_output is missing.")

    run_dir = _ensure_run_dir(state)
    data = _load_json(Path(state.pass3_output))
    lines: list[str] = []
    for date_entry in data.get("events_by_date", []):
        date_val = date_entry.get("event_date", "")
        event_strings = [_pass4_format_event(ev) for ev in date_entry.get("events", [])]
        lines.append(f"{date_val}|{'|||'.join(event_strings)}")

    compressed_text = "\n".join(lines)
    output_path = run_dir / "pass4_compressed.txt"
    output_path.write_text(compressed_text, encoding="utf-8")
    end_time = time.time()
    logger.info(f"Pass4 compress node completed in {end_time - start_time} seconds")
    return {
        "pass4_output": str(output_path),
        "compressed_timeline_text": compressed_text,
    }


# ----------------------------
# Pass5 node
# ----------------------------


class _Pass5Detail(BaseModel):
    date: str


class _Pass5Contradiction(BaseModel):
    contradiction_id: str
    dates_involved: list[str] = Field(default_factory=list)
    contradiction_type: str
    detail_a: _Pass5Detail
    detail_b: _Pass5Detail
    explanation: str
    severity: Literal["high", "medium", "low"]


class _Pass5Output(BaseModel):
    contradictions: list[_Pass5Contradiction] = Field(default_factory=list)


async def pass5_detect_contradictions_node(state: FraudTimelineWorkflowState) -> dict:
    """
    Detect contradictions in the node.
    """
    logger.info(f"Pass5 detect contradictions node started for {state.compressed_timeline_text}")
    start_time = time.time()
    compressed = state.compressed_timeline_text
    if not compressed:
        raise ValueError("compressed_timeline_text is missing.")

    run_dir = _ensure_run_dir(state)
    prompt_template = _load_text(PASS5_PROMPT_PATH)
    prompt_text = prompt_template.replace("{COMPRESSED_TIMELINE}", compressed)

    llm = _get_llm(0.0)
    response = await llm.ainvoke(prompt_text)
    content = response.content if isinstance(response.content, str) else str(response.content)

    parsed = _safe_extract_json_object(content)
    validated = _Pass5Output.model_validate(parsed).model_dump()

    output_path = run_dir / "pass5_output.json"
    _dump_json(output_path, validated)
    end_time = time.time()
    logger.info(f"Pass5 detect contradictions node completed in {end_time - start_time} seconds")
    return {"pass5_output": str(output_path)}

