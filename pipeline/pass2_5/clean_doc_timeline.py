"""Clean and flag document timeline for insurance review (Pass 2.5)."""

import asyncio
import json
import os
from time import time
from typing import Any, Dict, List, Literal

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, ValidationError

load_dotenv()

# =========================
# Config
# =========================

# Batch by DATE groups to control token usage.
BATCH_SIZE_DATES = 10
MAX_CONCURRENT_LLM_CALLS = 5

INPUT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input3_pass2.json"
PROMPT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/pipeline/pass2_5/clean_doc_prompt.md"
OUTPUT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input3_pass2_5.json"

# =========================
# Schemas (LLM output target)
# =========================


class EventFlag(BaseModel):
    """Represents a flag associated with an event."""

    flag_type: str
    severity: Literal["high", "medium", "low"]
    detail: str
    insurance_relevance: str


class CleanEvent(BaseModel):
    """Represents a single event in the document timeline."""

    event_tags: List[str] = Field(default_factory=list)
    event_details: List[str] = Field(default_factory=list)
    event_flags: List[EventFlag] = Field(default_factory=list)


class CleanEventsByDate(BaseModel):
    """Represents a list of events for a given date."""

    event_date: str
    events: List[CleanEvent] = Field(default_factory=list)


class CleanDocumentTimeline(BaseModel):
    """Represents the complete document timeline."""

    events_by_date: List[CleanEventsByDate] = Field(default_factory=list)


# =========================
# Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    """Load the prompt from the markdown file."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_pass2_document(file_path: str) -> Dict[str, Any]:
    """Load the pass2 document from the file."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "events_by_date" not in data:
        raise ValueError("Expected input JSON object with 'events_by_date' key.")
    if not isinstance(data["events_by_date"], list):
        raise ValueError("'events_by_date' must be a list.")

    return data


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    """Chunk the items into smaller lists."""
    return [items[i : i + size] for i in range(0, len(items), size)]


def safe_extract_json_object(text: str) -> Dict[str, Any]:
    """
    Robustly parse JSON object from LLM output.
    Supports occasional code fences or extra text.
    """
    cleaned = text.strip()

    # remove fenced markdown if present
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()

    # direct parse first
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # fallback: slice from first "{" to last "}"
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM output did not contain a valid JSON object.")

    candidate = cleaned[start : end + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("Parsed JSON is not an object.")
    return parsed


def normalize_for_prompt(batch_dates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Keep exactly the structure expected by clean_doc_prompt.md input contract:
    {
      "events_by_date": [
        {"date": "...", "events": [{"event_time": ..., "event_tag": ..., "event_summary": ...}]}
      ]
    }
    """
    normalized: List[Dict[str, Any]] = []

    for day in batch_dates:
        date_val = day.get("date")
        if not date_val:
            continue

        day_events: List[Dict[str, Any]] = []
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


def convert_clean_to_pass2_shape(clean_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert clean output back to pass2-like structure so we can re-merge pairs
    with the same prompt during binary merge.
    """
    converted_days: List[Dict[str, Any]] = []

    for day in clean_json.get("events_by_date", []):
        date_val = day.get("event_date")
        events_out: List[Dict[str, Any]] = []

        for ev in day.get("events", []):
            tags = ev.get("event_tags", [])
            details = ev.get("event_details", [])

            # choose a stable primary tag for re-input; keep others in summary text
            primary_tag = tags[0] if tags else "other"
            summary = "; ".join(details).strip() if details else "No details provided"

            if len(tags) > 1:
                summary = f"[tags: {', '.join(tags)}] {summary}"

            events_out.append(
                {
                    "event_time": None,  # pass2_5 output has no event_time; keep null
                    "event_tag": primary_tag,
                    "event_summary": summary,
                }
            )

        converted_days.append({"date": date_val, "events": events_out})

    return {"events_by_date": converted_days}


# =========================
# LLM setup
# =========================

llm = ChatOpenAI(
    model="gpt-4.1-mini",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.0,
)

PROMPT_TEMPLATE = load_prompt_from_md(PROMPT_PATH)


async def clean_batch_with_llm(
    batch_input_json: Dict[str, Any],
) -> CleanDocumentTimeline:
    """Clean a batch of events with the LLM."""
    input_block = json.dumps(batch_input_json, ensure_ascii=False, indent=2)
    prompt_text = PROMPT_TEMPLATE.replace("{PASS_2_JSON}", input_block)

    response = await llm.ainvoke(prompt_text)
    content = (
        response.content if isinstance(response.content, str) else str(response.content)
    )

    parsed = safe_extract_json_object(content)
    timeline = CleanDocumentTimeline.model_validate(parsed)
    return timeline


async def merge_pair(
    node_a: CleanDocumentTimeline,
    node_b: CleanDocumentTimeline,
    semaphore: asyncio.Semaphore,
) -> CleanDocumentTimeline:
    """
    Merge two clean nodes by converting to pass2-like shape and re-cleaning once.
    """
    combined_pass2_like = {"events_by_date": []}
    combined_pass2_like["events_by_date"].extend(
        convert_clean_to_pass2_shape(node_a.model_dump()).get("events_by_date", [])
    )
    combined_pass2_like["events_by_date"].extend(
        convert_clean_to_pass2_shape(node_b.model_dump()).get("events_by_date", [])
    )

    # keep chronological order for deterministic behavior
    combined_pass2_like["events_by_date"].sort(key=lambda d: d.get("date", ""))

    async with semaphore:
        return await clean_batch_with_llm(combined_pass2_like)


async def merge_level(
    nodes: List[CleanDocumentTimeline],
    semaphore: asyncio.Semaphore,
) -> List[CleanDocumentTimeline]:
    """Merge a level of nodes."""
    tasks: List[asyncio.Task] = []
    next_level: List[CleanDocumentTimeline] = []

    i = 0
    while i < len(nodes):
        if i + 1 >= len(nodes):
            next_level.append(nodes[i])  # odd node passes through
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


async def binary_merge_all(nodes: List[CleanDocumentTimeline]) -> CleanDocumentTimeline:
    """Merge all nodes in a binary tree."""
    if not nodes:
        return CleanDocumentTimeline(events_by_date=[])

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_LLM_CALLS)
    level = 1

    while len(nodes) > 1:
        print(f"Merge level {level}: {len(nodes)} node(s)")
        nodes = await merge_level(nodes, semaphore)
        level += 1

    return nodes[0]


# =========================
# Main processing
# =========================


async def process_document(file_path: str) -> CleanDocumentTimeline:
    """Process the document timeline."""
    pass2_json = load_pass2_document(file_path)
    by_date = pass2_json["events_by_date"]

    # sort dates for deterministic batching
    by_date_sorted = sorted(by_date, key=lambda d: d.get("date", ""))
    date_batches = chunked(by_date_sorted, BATCH_SIZE_DATES)

    print(f"Loaded {len(by_date_sorted)} date group(s)")
    print(f"Processing in {len(date_batches)} batch(es) of size {BATCH_SIZE_DATES}")

    # Step 1: clean each batch independently
    batch_nodes: List[CleanDocumentTimeline] = []
    for idx, batch in enumerate(date_batches, start=1):
        normalized = normalize_for_prompt(batch)
        event_count = sum(
            len(d.get("events", [])) for d in normalized["events_by_date"]
        )
        print(
            f"Batch {idx}: {len(normalized['events_by_date'])} date(s), {event_count} event(s)"
        )

        cleaned = await clean_batch_with_llm(normalized)
        cleaned_count = sum(len(day.events) for day in cleaned.events_by_date)
        print(
            f"Batch {idx}: LLM returned {len(cleaned.events_by_date)} date(s), {cleaned_count} grouped event(s)"
        )
        batch_nodes.append(cleaned)

    # Step 2: merge across batches to deduplicate globally
    final_clean = await binary_merge_all(batch_nodes)

    # final stable sort by event_date
    final_clean.events_by_date.sort(key=lambda d: d.event_date)

    return final_clean


if __name__ == "__main__":
    """Main function."""
    start = time()

    try:
        final_timeline = asyncio.run(process_document(INPUT_PATH))
    except ValidationError as e:
        print("Validation error from LLM output:")
        print(e)
        raise
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_timeline.model_dump(), f, indent=2, ensure_ascii=False)

    end = time()
    total_events = sum(len(day.events) for day in final_timeline.events_by_date)

    print(f"Saved cleaned document timeline to: {OUTPUT_PATH}")
    print(f"Total dates: {len(final_timeline.events_by_date)}")
    print(f"Total grouped events: {total_events}")
    print(f"Completed in {end - start:.2f}s")
