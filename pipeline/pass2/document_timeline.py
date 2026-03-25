"""Build final document-level timeline from chunk-level extracted events."""

import asyncio
import json
import os
from time import time
from typing import Any, Dict, List, Literal, Optional

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

load_dotenv()

# =========================
# Config
# =========================

BATCH_SIZE = 2

MAX_CONCURRENT_BATCH_CLEAN_CALLS = 5
MAX_CONCURRENT_MERGE_CALLS = 5

INPUT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input2_pass1.json"
PROMPT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/pipeline/pass2/merging_prompt.md"
)
OUTPUT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "rough_jsons/15308/input2_pass2.json"
)

# =========================
# Pydantic Schemas (final output)
# =========================


class Event(BaseModel):
    """
    Represents a single event in the document timeline.
    """

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
    """
    Represents a list of events for a given date.
    """

    date: str  # YYYY-MM-DD
    events: List[Event] = Field(default_factory=list)


class DocumentTimeline(BaseModel):
    """
    Represents the complete document timeline.
    """

    events_by_date: List[EventsByDate] = Field(default_factory=list)


# =========================
# Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    """
    Load the prompt from the markdown file.
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_chunk_results(file_path: str) -> List[Dict[str, Any]]:
    """
    Load the chunk results from the JSON file.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected input JSON as a list of chunk timeline objects.")
    return data


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    """
    Chunk the items into a list of lists.
    """
    return [items[i : i + size] for i in range(0, len(items), size)]


def normalize_time(value: Optional[str]) -> str:
    """
    Normalize the time value.
    """
    return "null" if value in (None, "", "null", "None") else str(value).strip()


def flatten_chunks_to_lines(chunks: List[Dict[str, Any]]) -> List[str]:
    """
    Input chunk shape:
      chunk -> events_by_date[] -> events[]
    Output lines:
      DATE | TIME | TAG | SUMMARY
    """
    lines: List[str] = []

    for chunk_obj in chunks:
        for by_date in chunk_obj.get("events_by_date", []):
            date = by_date.get("date")
            if not date:
                continue

            for ev in by_date.get("events", []):
                event_time = normalize_time(ev.get("event_time"))
                tag = (ev.get("event_tag") or "").strip()
                summary = (ev.get("event_summary") or "").strip()

                if not tag or not summary:
                    continue

                lines.append(f"{date} | {event_time} | {tag} | {summary}")

    return lines


def parse_llm_lines(raw_text: str) -> List[Dict[str, Optional[str]]]:
    """
    Parse LLM output lines in format:
      DATE | TIME | TAG | SUMMARY
    """
    parsed: List[Dict[str, Optional[str]]] = []

    for line in raw_text.splitlines():
        clean = line.strip()
        if not clean:
            continue

        # Split at first 3 delimiters, summary can contain pipes.
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


def group_events_by_date(events: List[Dict[str, Optional[str]]]) -> List[EventsByDate]:
    """
    Group events by date.
    """
    grouped: Dict[str, List[Event]] = {}

    for e in events:
        date = str(e["date"])
        grouped.setdefault(date, []).append(
            Event(
                event_time=e["event_time"],
                event_tag=e["event_tag"],  # validated by Literal
                event_summary=e["event_summary"],
            )
        )

    # Sort: date asc; within date time asc; null time last
    result: List[EventsByDate] = []
    for date in sorted(grouped.keys()):
        day_events = grouped[date]
        day_events.sort(
            key=lambda ev: (
                ev.event_time is None,  # False first (timed), True later (null)
                ev.event_time or "99:99",
            )
        )
        result.append(EventsByDate(date=date, events=day_events))

    return result


# =========================
# LLM setup
# =========================

llm = ChatOpenAI(
    model="gpt-4.1-mini",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.0,
)

PROMPT_TEMPLATE = load_prompt_from_md(PROMPT_PATH)


async def merge_event_lines(event_lines: List[str]) -> List[str]:
    """
    Send pipe-delimited lines to LLM and return cleaned pipe-delimited lines.
    """
    if not event_lines:
        return []

    input_block = "\n".join(event_lines)
    prompt_text = PROMPT_TEMPLATE.replace("{{EVENTS}}", input_block)

    response = await llm.ainvoke(prompt_text)
    content = (
        response.content if isinstance(response.content, str) else str(response.content)
    )

    # Keep non-empty lines only
    return [line.strip() for line in content.splitlines() if line.strip()]


async def merge_pair(
    lines_a: List[str], lines_b: List[str], semaphore: asyncio.Semaphore
) -> List[str]:
    """
    Merge two lists of lines.
    """
    async with semaphore:
        return await merge_event_lines(lines_a + lines_b)


async def merge_level(
    nodes: List[List[str]], semaphore: asyncio.Semaphore
) -> List[List[str]]:
    """
    Merge a level of nodes.
    """
    tasks: List[asyncio.Task] = []
    next_level: List[List[str]] = []

    i = 0
    while i < len(nodes):
        if i + 1 >= len(nodes):
            next_level.append(nodes[i])  # odd one passes through
            break

        tasks.append(asyncio.create_task(merge_pair(nodes[i], nodes[i + 1], semaphore)))
        i += 2

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                raise r
            next_level.append(r)

    return next_level


async def binary_merge_all(nodes: List[List[str]]) -> List[str]:
    """
    Binary-tree merge to remove duplicates across all batch outputs.
    """
    if not nodes:
        return []

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_MERGE_CALLS)
    level = 1

    while len(nodes) > 1:
        print(f"Merge level {level}: {len(nodes)} node(s)")
        nodes = await merge_level(nodes, semaphore)
        level += 1

    return nodes[0]


async def clean_one_batch(
    idx: int,
    batch: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> List[str]:
    """
    Clean one batch of chunks.
    """
    flat_lines = flatten_chunks_to_lines(batch)
    print(f"Batch {idx}: flattened {len(flat_lines)} event line(s)")

    if not flat_lines:
        print(f"Batch {idx}: LLM returned 0 cleaned line(s)")
        return []

    async with semaphore:
        cleaned_lines = await merge_event_lines(flat_lines)

    print(f"Batch {idx}: LLM returned {len(cleaned_lines)} cleaned line(s)")
    return cleaned_lines


# =========================
# Main processing
# =========================


async def process_document(file_path: str) -> DocumentTimeline:
    """
    Process the document.
    """
    chunks = load_chunk_results(file_path)
    batches = chunked(chunks, BATCH_SIZE)

    print(f"Loaded {len(chunks)} chunks")
    print(f"Processing in {len(batches)} batch(es) of size {BATCH_SIZE}")

    # Step 1: flatten + merge each batch (parallel with limit)
    batch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCH_CLEAN_CALLS)

    tasks = [
        asyncio.create_task(clean_one_batch(idx, batch, batch_semaphore))
        for idx, batch in enumerate(batches, start=1)
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    batch_nodes: List[List[str]] = []
    for idx, r in enumerate(results, start=1):
        if isinstance(r, Exception):
            raise RuntimeError(f"Batch {idx} failed: {r}") from r
        batch_nodes.append(r)

    # Step 2: merge across batches (global dedup)
    final_lines = await binary_merge_all(batch_nodes)
    print(f"Final cleaned line count: {len(final_lines)}")

    # Step 3: parse lines -> final JSON structure
    parsed_events = parse_llm_lines("\n".join(final_lines))
    events_by_date = group_events_by_date(parsed_events)

    return DocumentTimeline(events_by_date=events_by_date)


if __name__ == "__main__":
    start = time()
    final_timeline = asyncio.run(process_document(INPUT_PATH))
    end = time()

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_timeline.model_dump(), f, indent=2, ensure_ascii=False)

    print(f"Saved final timeline to: {OUTPUT_PATH}")
    print(f"Total dates: {len(final_timeline.events_by_date)}")
    print(f"Completed in {end - start:.2f}s")
