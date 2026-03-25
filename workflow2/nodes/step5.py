"""Step 5: conflict detection (intra-group + binary-tree cross-group)."""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from loguru import logger
import time

load_dotenv()


# =========================
# Config
# =========================

DEFAULT_GROUP_SIZE = 10
DEFAULT_MAX_CONCURRENT = 40
DEFAULT_OUTPUT_DIR = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons"
DEFAULT_OUTPUT_FILENAME = "conflicts.json"

DEFAULT_PROMPT1_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/prompts/conflict_prompt1.md"
DEFAULT_PROMPT2_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/prompts/conflict_prompt2.md"


# =========================
# Pydantic Schemas
# =========================


class SourceEvent(BaseModel):
    date: str
    chunk_ids: List[str] = Field(default_factory=list)
    event: str  # conflicting fragments only


class Conflict(BaseModel):
    conflict_type: Literal[
        "DUPLICATE_PROCEDURE",
        "PROVIDER_CONFLICT",
        "ANATOMY_SIDE_MISMATCH",
        "TIMELINE_IMPOSSIBILITY",
        "INJURY_MECHANISM_CONFLICT",
        "DOSAGE_DISCREPANCY",
        "DIAGNOSIS_PROCEDURE_MISMATCH",
        "VAGUE_PROCEDURE_CODE",
        "MISSING_PROVIDER",
        "WITHIN_DOC_DUPLICATE",
        "METADATA_EVENT",
    ]
    severity: Literal["high", "medium", "low"]
    dates_involved: List[str] = Field(default_factory=list)
    conflict_brief: str
    source_events: List[SourceEvent] = Field(default_factory=list)


class ConflictOutput(BaseModel):
    conflicts: List[Conflict] = Field(default_factory=list)


# =========================
# IO Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_step4_entries(file_path: str) -> List[Dict[str, Any]]:
    """
    Accept either:
    - {"events_by_date": [...]}
    - [...]
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and isinstance(data.get("events_by_date"), list):
        entries = data["events_by_date"]
    elif isinstance(data, list):
        entries = data
    else:
        raise ValueError("Expected step4 output as {'events_by_date': [...]} or list.")

    clean = [e for e in entries if isinstance(e, dict) and e.get("date")]
    clean.sort(key=lambda x: parse_date_safe(x.get("date")))
    return clean


# =========================
# Normalize + Flatten Helpers
# =========================


def parse_date_safe(date_str: Any) -> datetime:
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d")
    except Exception:
        return datetime.max


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").strip()


def normalize_details(value: Any) -> List[str]:
    """
    Preferred: event_details as List[str]
    Legacy fallback: event as bullet/newline string
    """
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            t = safe_text(item).lstrip("•").strip()
            if t:
                out.append(t)
        return out

    if isinstance(value, str):
        lines = [ln.strip() for ln in value.split("\n") if ln.strip()]
        return [ln.lstrip("•").strip() for ln in lines if ln.strip()]

    return []


def flatten_dates(date_entries: List[Dict[str, Any]]) -> str:
    """
    DATE || EVENT_TAG || DETAIL_1 ## DETAIL_2 || CHUNK1^^CHUNK2
    """
    rows = sorted(
        [d for d in date_entries if isinstance(d, dict) and d.get("date")],
        key=lambda x: parse_date_safe(x.get("date")),
    )

    lines: List[str] = []

    for day in rows:
        date = safe_text(day.get("date"))
        events = day.get("events", [])
        if not date or not isinstance(events, list):
            continue

        for ev in events:
            if not isinstance(ev, dict):
                continue

            event_tag = safe_text(ev.get("event_tag")) or "other"

            details = normalize_details(ev.get("event_details"))
            if not details:
                details = normalize_details(ev.get("event"))  # fallback

            chunk_ids_raw = ev.get("chunk_ids", [])
            chunk_ids: List[str] = []
            if isinstance(chunk_ids_raw, list):
                chunk_ids = [safe_text(c) for c in chunk_ids_raw if safe_text(c)]

            details_col = " ## ".join(details)
            chunks_col = "^^".join(chunk_ids)

            lines.append(f"{date} || {event_tag} || {details_col} || {chunks_col}")

    return "\n".join(lines)


def split_into_groups(
    date_entries: List[Dict[str, Any]],
    group_size: int,
) -> List[List[Dict[str, Any]]]:
    if group_size <= 0:
        group_size = DEFAULT_GROUP_SIZE
    return [
        date_entries[i : i + group_size]
        for i in range(0, len(date_entries), group_size)
    ]


# =========================
# LLM Factory
# =========================


def build_chains(prompt1_path: str, prompt2_path: str):
    llm = ChatOpenAI(
        model="gpt-4.1-mini",
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
    ).with_structured_output(ConflictOutput)

    prompt1 = ChatPromptTemplate.from_template(load_prompt_from_md(prompt1_path))
    prompt2 = ChatPromptTemplate.from_template(load_prompt_from_md(prompt2_path))

    return prompt1 | llm, prompt2 | llm


# =========================
# LLM Calls
# =========================


async def run_with_semaphore(coro, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            return await coro
        except Exception:
            return None


async def run_chain1_for_group(chain1, group: List[Dict[str, Any]]) -> List[Conflict]:
    timeline_lines = flatten_dates(group)
    if not timeline_lines.strip():
        return []
    response: ConflictOutput = await chain1.ainvoke({"timeline_lines": timeline_lines})
    return response.conflicts


async def run_chain2_for_pair(
    chain2,
    left_group: List[Dict[str, Any]],
    right_group: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Conflict]]:
    merged = sorted(
        [*left_group, *right_group],
        key=lambda x: parse_date_safe(x.get("date")),
    )

    timeline_lines = flatten_dates(merged)
    if not timeline_lines.strip():
        return merged, []

    response: ConflictOutput = await chain2.ainvoke({"timeline_lines": timeline_lines})
    return merged, response.conflicts


# =========================
# Phase 1 + Phase 2
# =========================


async def phase1_intra_group(
    chain1,
    groups: List[List[Dict[str, Any]]],
    max_concurrent: int,
) -> List[Conflict]:
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        run_with_semaphore(run_chain1_for_group(chain1, g), semaphore) for g in groups
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: List[Conflict] = []
    for r in results:
        if isinstance(r, Exception) or not r:
            continue
        out.extend(r)
    return out


async def phase2_binary_tree(
    chain2,
    groups: List[List[Dict[str, Any]]],
    max_concurrent: int,
) -> List[Conflict]:
    current_level = groups[:]
    all_conflicts: List[Conflict] = []

    while len(current_level) > 1:
        semaphore = asyncio.Semaphore(max_concurrent)
        next_level: List[List[Dict[str, Any]]] = []
        tasks = []

        i = 0
        while i < len(current_level):
            if i + 1 < len(current_level):
                tasks.append(
                    run_with_semaphore(
                        run_chain2_for_pair(
                            chain2, current_level[i], current_level[i + 1]
                        ),
                        semaphore,
                    )
                )
                i += 2
            else:
                next_level.append(current_level[i])  # carry forward odd group
                i += 1

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception) or not r:
                continue
            merged_group, conflicts = r
            next_level.append(merged_group)
            if conflicts:
                all_conflicts.extend(conflicts)

        current_level = next_level

    return all_conflicts


# =========================
# Finalize
# =========================


def assign_conflict_ids(conflicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for i, c in enumerate(conflicts, start=1):
        c["conflict_id"] = f"C{i:03d}"
    return conflicts


async def run_step5(
    step4_output_path: str,
    prompt1_path: str = DEFAULT_PROMPT1_PATH,
    prompt2_path: str = DEFAULT_PROMPT2_PATH,
    group_size: int = DEFAULT_GROUP_SIZE,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
) -> List[Dict[str, Any]]:
    date_entries = load_step4_entries(step4_output_path)
    if not date_entries:
        return []

    groups = split_into_groups(date_entries, group_size)
    chain1, chain2 = build_chains(prompt1_path, prompt2_path)

    phase1_conflicts = await phase1_intra_group(chain1, groups, max_concurrent)
    phase2_conflicts = await phase2_binary_tree(chain2, groups, max_concurrent)

    all_conflicts = [*phase1_conflicts, *phase2_conflicts]
    conflicts_dicts = [c.model_dump() for c in all_conflicts]
    conflicts_dicts = assign_conflict_ids(conflicts_dicts)

    return conflicts_dicts


# =========================
# LangGraph Node Wrapper
# =========================


async def step5_detect_conflicts_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected state keys:
    - step4_output: str
    - run_label: str (optional, defaults to "run")
    - output_dir: str (optional)
    - prompt_step5_1_path: str (optional)
    - prompt_step5_2_path: str (optional)
    - step5_group_size: int (optional)
    - step5_max_concurrent: int (optional)
    - step5_output_filename: str (optional)
    """
    step4_output = state.step4_output
    if not isinstance(step4_output, str) or not step4_output:
        raise ValueError("state['step4_output'] is required for step5.")

    run_label = state.run_label
    output_dir = state.output_dir
    output_filename = state.step5_output_filename

    prompt1_path = state.prompt_step5_1_path or DEFAULT_PROMPT1_PATH
    prompt2_path = state.prompt_step5_2_path or DEFAULT_PROMPT2_PATH
    group_size = state.step5_group_size
    max_concurrent = state.step5_max_concurrent

    start_time = time.time()
    conflicts = await run_step5(
        step4_output_path=step4_output,
        prompt1_path=prompt1_path,
        prompt2_path=prompt2_path,
        group_size=group_size,
        max_concurrent=max_concurrent,
    )

    end_time = time.time()
    logger.info(f"Step5 completed in {end_time - start_time:.2f} seconds")

    output_path = Path(output_dir) / run_label / output_filename
    save_json(output_path, conflicts)

    return {
        "step5_output": str(output_path),
        "step5_conflict_count": len(conflicts),
    }
