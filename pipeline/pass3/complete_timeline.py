"""Pass 3: Merge same-date events into one date object per day."""

import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from time import time
from typing import Any, Dict, List, Literal

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, ValidationError

load_dotenv()

# =========================
# Config
# =========================

MAX_PARALLEL = 5
LARGE_DATE_EVENT_THRESHOLD = 10

INPUT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "rough_jsons/15308/merged_pass2_5.json"
)
PROMPT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/pipeline/pass3/prompt.md"
OUTPUT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "rough_jsons/15308/pass3_output.json"
)

TAG_ORDER = [
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
TAG_RANK = {tag: idx for idx, tag in enumerate(TAG_ORDER)}

# =========================
# Schemas
# =========================


class EventFlag(BaseModel):
    """Represents a flag associated with an event."""

    flag_type: str
    severity: Literal["high", "medium", "low"]
    detail: str
    insurance_relevance: str


class Event(BaseModel):
    """Represents a single event in the document timeline."""

    event_tags: List[str] = Field(default_factory=list)
    event_details: List[str] = Field(default_factory=list)
    event_flags: List[EventFlag] = Field(default_factory=list)


class EventsByDate(BaseModel):
    """Represents a list of events for a given date."""

    event_date: str
    events: List[Event] = Field(default_factory=list)


class FinalTimeline(BaseModel):
    """Represents the complete document timeline."""

    events_by_date: List[EventsByDate] = Field(default_factory=list)


# =========================
# Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    """Load the prompt from the markdown file."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_input(file_path: str) -> Dict[str, Any]:
    """Load the input from the json file."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "events_by_date" not in data:
        raise ValueError("Expected input JSON object with 'events_by_date'.")
    if not isinstance(data["events_by_date"], list):
        raise ValueError("'events_by_date' must be a list.")
    return data


def parse_date_safe(date_str: str) -> datetime:
    """Parse the date safely."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return datetime.max


def group_events_by_date(
    entries: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Step 1 in task.md:
    Group all events from duplicate date-objects into a single date bucket.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for entry in entries:
        date_val = entry.get("event_date")
        if not date_val:
            continue

        for event in entry.get("events", []):
            if isinstance(event, dict):
                grouped[date_val].append(event)

    return grouped


def event_tag_rank(event: Dict[str, Any]) -> int:
    """Get the rank of the event tags."""
    tags = event.get("event_tags", [])
    if not isinstance(tags, list) or not tags:
        return len(TAG_ORDER) + 1

    ranks = [TAG_RANK.get(str(t), len(TAG_ORDER) + 1) for t in tags]
    return min(ranks) if ranks else len(TAG_ORDER) + 1


def sort_events_for_merge(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Pre-sort events before merging so similar tags cluster together.
    """
    return sorted(events, key=event_tag_rank)


def safe_extract_json_array(text: str) -> List[Dict[str, Any]]:
    """
    Parse LLM output expected as raw JSON array.
    Also tolerates occasional fenced output.
    """
    cleaned = text.strip()

    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    # direct parse
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    # fallback by slicing first '[' to last ']'
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM output did not contain a valid JSON array.")

    candidate = cleaned[start : end + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, list):
        raise ValueError("Parsed JSON is not an array.")
    return parsed


# =========================
# LLM Setup
# =========================

llm = ChatOpenAI(
    model="gpt-4.1-mini",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.0,
)

PROMPT_TEMPLATE = load_prompt_from_md(PROMPT_PATH)


async def merge_events_once(
    events_a: List[Dict[str, Any]],
    events_b: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> List[Dict[str, Any]]:
    """
    Single LLM merge call for two event lists on same date.
    Prompt expects exactly:
      {"events_a": [...], "events_b": [...]}
    """
    payload = {
        "events_a": events_a,
        "events_b": events_b,
    }

    prompt_text = PROMPT_TEMPLATE.replace(
        "{EVENTS_JSON}",
        json.dumps(payload, ensure_ascii=False, indent=2),
    )

    async with semaphore:
        response = await llm.ainvoke(prompt_text)

    content = (
        response.content if isinstance(response.content, str) else str(response.content)
    )
    merged_list = safe_extract_json_array(content)

    # Validate shape early
    validated = [Event.model_validate(item).model_dump() for item in merged_list]
    return validated


async def merge_level(
    nodes: List[List[Dict[str, Any]]],
    semaphore: asyncio.Semaphore,
) -> List[List[Dict[str, Any]]]:
    """
    One binary-tree level:
    [n1, n2, n3, n4, n5] -> [merge(n1,n2), merge(n3,n4), n5]
    """
    tasks: List[asyncio.Task] = []
    next_level: List[List[Dict[str, Any]]] = []

    i = 0
    while i < len(nodes):
        if i + 1 >= len(nodes):
            next_level.append(nodes[i])  # odd node pass-through
            break

        tasks.append(
            asyncio.create_task(merge_events_once(nodes[i], nodes[i + 1], semaphore))
        )
        i += 2

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                raise r
            next_level.append(r)

    return next_level


async def binary_merge_events(
    events: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> List[Dict[str, Any]]:
    """
    Pairwise tree merge for large date buckets.
    Each node is a list of event objects.
    """
    if not events:
        return []

    nodes: List[List[Dict[str, Any]]] = [[e] for e in events]
    level = 1

    while len(nodes) > 1:
        print(f"  merge level {level}: {len(nodes)} node(s)")
        nodes = await merge_level(nodes, semaphore)
        level += 1

    return nodes[0]


# =========================
# Date Processing
# =========================


async def process_single_date(
    event_date: str,
    events: List[Dict[str, Any]],
    semaphore: asyncio.Semaphore,
) -> EventsByDate:
    """
    Step 2 + Step 3 in task.md.
    """
    # skip LLM if only one event
    if len(events) == 1:
        single = Event.model_validate(events[0]).model_dump()
        return EventsByDate(
            event_date=event_date, events=[Event.model_validate(single)]
        )

    ordered_events = sort_events_for_merge(events)

    if len(ordered_events) > LARGE_DATE_EVENT_THRESHOLD:
        merged_events = await binary_merge_events(ordered_events, semaphore)
    else:
        # one-shot merge for small buckets (2..10)
        mid = len(ordered_events) // 2
        events_a = ordered_events[:mid]
        events_b = ordered_events[mid:]
        merged_events = await merge_events_once(events_a, events_b, semaphore)

    validated_events = [Event.model_validate(ev) for ev in merged_events]
    return EventsByDate(event_date=event_date, events=validated_events)


async def process_timeline(input_path: str) -> FinalTimeline:
    data = load_input(input_path)
    grouped = group_events_by_date(data["events_by_date"])

    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    # Process date buckets sequentially for easier debugging/logging.
    # (You can parallelize per-date later if needed.)
    final_dates: List[EventsByDate] = []
    for date_key in sorted(grouped.keys(), key=parse_date_safe):
        events = grouped[date_key]
        print(f"Processing {date_key}: {len(events)} event object(s)")

        processed = await process_single_date(date_key, events, semaphore)
        final_dates.append(processed)

    return FinalTimeline(events_by_date=final_dates)


if __name__ == "__main__":
    start = time()

    try:
        final_timeline = asyncio.run(process_timeline(INPUT_PATH))
    except ValidationError as e:
        print("Validation error from model output:")
        print(e)
        raise
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_timeline.model_dump(), f, indent=2, ensure_ascii=False)

    end = time()
    total_dates = len(final_timeline.events_by_date)
    total_events = sum(len(d.events) for d in final_timeline.events_by_date)

    print(f"Saved pass3 output to: {OUTPUT_PATH}")
    print(f"Total unique dates: {total_dates}")
    print(f"Total merged events: {total_events}")
    print(f"Completed in {end - start:.2f}s")
