"""Step 4: deduplicate and merge date-level events into final timeline."""

import asyncio
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

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

MAX_CONCURRENT = 40
DEFAULT_OUTPUT_DIR = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons"
DEFAULT_PROMPT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/prompts/timeline_prompt.md"
DEFAULT_OUTPUT_FILENAME = "pass2_output.json"


# =========================
# Pydantic Schemas
# =========================


class MergedEvent(BaseModel):
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
    event_details: List[str] = Field(default_factory=list)
    chunk_ids: List[str] = Field(default_factory=list)


class DayTimeline(BaseModel):
    date: str
    events: List[MergedEvent] = Field(default_factory=list)


class Pass2Output(BaseModel):
    events_by_date: List[DayTimeline] = Field(default_factory=list)


# =========================
# IO Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_combined_pass1(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected combined pass1 JSON as a top-level list.")
    return [row for row in data if isinstance(row, dict)]


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


# =========================
# Transform Helpers
# =========================


def normalize_time(value: Any) -> str:
    if value is None:
        return "null"
    text = str(value).strip()
    return text if text else "null"


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").strip()


def build_date_lines(entries: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build one prompt block per date:
    DATE | TIME | TAG | SUMMARY | CHUNK_ID
    """
    grouped_lines: Dict[str, List[str]] = defaultdict(list)

    for item in entries:
        date = safe_text(item.get("date"))
        if not date:
            continue

        chunk_id = safe_text(item.get("chunk")) or "unknown_chunk"
        events = item.get("events", [])
        if not isinstance(events, list):
            continue

        for ev in events:
            if not isinstance(ev, dict):
                continue

            event_time = normalize_time(ev.get("event_time"))
            event_tag = safe_text(ev.get("event_tag")) or "other"
            event_summary = safe_text(ev.get("event_summary"))
            if not event_summary:
                continue

            line = f"{date} | {event_time} | {event_tag} | {event_summary} | {chunk_id}"
            grouped_lines[date].append(line)

    return {date: "\n".join(lines) for date, lines in grouped_lines.items()}


# =========================
# LLM Factory
# =========================


def build_chain(prompt_path: str):
    llm = ChatOpenAI(
        model="gpt-5-mini",
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
    ).with_structured_output(Pass2Output)

    prompt = ChatPromptTemplate.from_template(load_prompt_from_md(prompt_path))
    return prompt | llm


# =========================
# Date Processing
# =========================


async def process_one_date(chain, date: str, date_lines: str) -> DayTimeline:
    response: Pass2Output = await chain.ainvoke({"date_lines": date_lines})

    if response.events_by_date:
        # Defensive: prompt should return one date, but normalize safely.
        for day in response.events_by_date:
            if day.date == date:
                return day
        return response.events_by_date[0]

    return DayTimeline(date=date, events=[])


async def process_one_date_with_limit(
    chain,
    date: str,
    date_lines: str,
    semaphore: asyncio.Semaphore,
) -> Optional[DayTimeline]:
    """
    Process a date with a limit on the number of concurrent calls.
    """
    async with semaphore:
        logger.info(f"Step4 processing date={date}")
        try:
            result = await process_one_date(chain, date, date_lines)
            logger.info(f"Step4 completed date={date}")
            return result
        except Exception as exc:
            logger.exception(f"Step4 failed date={date}: {exc}")
            return None


async def run_step4(
    step3_output_path: str,
    prompt_path: str = DEFAULT_PROMPT_PATH,
    max_concurrent: int = MAX_CONCURRENT,
) -> Pass2Output:
    entries = load_combined_pass1(step3_output_path)
    date_to_lines = build_date_lines(entries)
    logger.info(f"Step4 total_dates_to_process={len(date_to_lines)}")

    if not date_to_lines:
        return Pass2Output(events_by_date=[])

    chain = build_chain(prompt_path)
    semaphore = asyncio.Semaphore(max_concurrent)

    sorted_dates = sorted(date_to_lines.keys())
    tasks = [
        process_one_date_with_limit(chain, date, date_to_lines[date], semaphore)
        for date in sorted_dates
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    final_days: List[DayTimeline] = []
    for r in results:
        if isinstance(r, Exception):
            continue
        if r:
            final_days.append(r)

    final_days.sort(key=lambda d: d.date)
    logger.info(f"Step4 completed final_days={len(final_days)}")
    return Pass2Output(events_by_date=final_days)


# =========================
# LangGraph Node Wrapper
# =========================


async def step4_build_timeline_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected state keys:
    - step3_output: str
    - run_label: str (optional, defaults to "run")
    - output_dir: str (optional)
    - prompt_step4_path: str (optional)
    - step4_output_filename: str (optional)
    - step4_max_concurrent: int (optional)
    """
    step3_output = state.step3_output
    run_label = state.run_label
    output_dir = state.output_dir
    prompt_path = state.prompt_step4_path or DEFAULT_PROMPT_PATH
    output_filename = state.step4_output_filename
    max_concurrent = state.step4_max_concurrent

    if not step3_output or not isinstance(step3_output, str):
        raise ValueError("state['step3_output'] is required for step4.")

    start_time = time.time()
    result = await run_step4(
        step3_output_path=step3_output,
        prompt_path=prompt_path,
        max_concurrent=max_concurrent,
    )

    end_time = time.time()
    logger.info(f"Step4 completed in {end_time - start_time:.2f} seconds")

    output_path = Path(output_dir) / run_label / output_filename
    save_json(output_path, result.model_dump())

    return {"step4_output": str(output_path)}
