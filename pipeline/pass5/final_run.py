"""Pass 5: Detect cross-timeline contradictions from compressed pass3 timeline."""

import json
import os
from time import time
from typing import Any, Dict, List, Literal, Optional

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, ValidationError, ConfigDict

load_dotenv()

# =========================
# Config
# =========================

INPUT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "rough_jsons/15308/pass3_output.json"
)
PROMPT_PATH = "/home/ankit/smartsense_code/fraudx_timeline_poc/pipeline/pass5/prompt.md"
OUTPUT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "rough_jsons/15308/pass5_output.json"
)
COMPRESSED_OUTPUT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/"
    "rough_jsons/15308/pass5_compressed_input.txt"
)

MAX_COMPRESSED_CHARS: Optional[int] = None  # e.g. 120_000

# =========================
# Output Schemas
# =========================


class ContradictionDetail(BaseModel):
    # Ignore unexpected fields if model still returns legacy "text"
    model_config = ConfigDict(extra="ignore")
    date: str


class Contradiction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    contradiction_id: str
    dates_involved: List[str] = Field(default_factory=list)
    contradiction_type: str
    detail_a: ContradictionDetail
    detail_b: ContradictionDetail
    explanation: str
    severity: Literal["high", "medium", "low"]


class ContradictionOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")
    contradictions: List[Contradiction] = Field(default_factory=list)


# =========================
# IO + Parsing Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Input JSON must be an object.")
    return data


def safe_extract_json_object(text: str) -> Dict[str, Any]:
    cleaned = text.strip()

    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
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
        raise ValueError("LLM output did not contain a valid JSON object.")

    parsed = json.loads(cleaned[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Parsed JSON is not an object.")
    return parsed


# =========================
# Compression (pass4 style, in-memory)
# =========================

try:
    from pipeline.pass4.convert import format_event as pass4_format_event  # type: ignore
except Exception:

    def _format_flags(flags: list) -> str:
        if not flags:
            return "-"
        parts = []
        for f in flags:
            flag_type = f.get("flag_type", "")
            severity = f.get("severity", "")
            detail = f.get("detail", "")
            parts.append(f"{flag_type}[{severity}]:{detail}")
        return " || ".join(parts)

    def pass4_format_event(event: dict) -> str:
        tags = "+".join(event.get("event_tags", []))
        details = "; ".join(event.get("event_details", []))
        flags = _format_flags(event.get("event_flags", []))
        return f"{tags}|{details}|{flags}"


def compress_timeline_in_memory(data: Dict[str, Any]) -> str:
    lines: List[str] = []

    for date_entry in data.get("events_by_date", []):
        date_val = date_entry.get("event_date", "")
        events = date_entry.get("events", [])
        if not date_val or not isinstance(events, list):
            continue

        event_strings = [pass4_format_event(e) for e in events]
        lines.append(f"{date_val}|{'|||'.join(event_strings)}")

    compressed = "\n".join(lines)

    if MAX_COMPRESSED_CHARS and len(compressed) > MAX_COMPRESSED_CHARS:
        compressed = compressed[:MAX_COMPRESSED_CHARS]

    return compressed


# =========================
# LLM Pipeline
# =========================

llm = ChatOpenAI(
    model="gpt-4.1-mini",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.0,
)


def build_prompt(prompt_template: str, compressed_timeline: str) -> str:
    # Only inject timeline. contradiction types are already in prompt.md.
    return prompt_template.replace("{COMPRESSED_TIMELINE}", compressed_timeline)


def detect_contradictions(prompt_text: str) -> ContradictionOutput:
    response = llm.invoke(prompt_text)
    content = (
        response.content if isinstance(response.content, str) else str(response.content)
    )
    parsed = safe_extract_json_object(content)
    return ContradictionOutput.model_validate(parsed)


def run_pipeline(input_path: str, prompt_path: str) -> ContradictionOutput:
    data = load_json(input_path)
    compressed = compress_timeline_in_memory(data)

    # NEW: save compressed timeline text for debugging/audit
    with open(COMPRESSED_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(compressed)

    prompt_template = load_prompt_from_md(prompt_path)
    prompt_text = build_prompt(prompt_template, compressed)

    return detect_contradictions(prompt_text)


if __name__ == "__main__":
    start = time()

    try:
        result = run_pipeline(INPUT_PATH, PROMPT_PATH)
    except ValidationError as e:
        print("Validation error from LLM output:")
        print(e)
        raise
    except Exception as e:
        print(f"Pass5 pipeline failed: {e}")
        raise

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result.model_dump(), f, indent=2, ensure_ascii=False)

    end = time()
    print(f"Saved contradictions to: {OUTPUT_PATH}")
    print(f"Saved compressed input to: {COMPRESSED_OUTPUT_PATH}")
    print(f"Total contradictions: {len(result.contradictions)}")
    print(f"Completed in {end - start:.2f}s")
