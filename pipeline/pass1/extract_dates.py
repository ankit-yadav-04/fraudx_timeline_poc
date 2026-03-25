"""Extract timeline events from document chunks"""

import json
import os
import asyncio
from typing import Any, Dict, List, Literal, Optional
from time import time

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate


load_dotenv()


# =========================
# Config
# =========================

MAX_CONCURRENT_CHUNK_CALLS = 20


# =========================
# Pydantic Schemas
# =========================


class Event(BaseModel):
    """Single medical event"""

    event_time: Optional[str] = None  # HH:MM

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
    """Events grouped by date"""

    date: str  # YYYY-MM-DD
    events: List[Event]


class ChunkEvents(BaseModel):
    """LLM output schema (only extracted data)"""

    events_by_date: List[EventsByDate] = Field(default_factory=list)


class ChunkTimeline(BaseModel):
    """Final output with chunk metadata"""

    # boundingBoxes: List = Field(default_factory=list)
    chunk: str
    chunk_number: int
    pageNumbers: List[int] = Field(default_factory=list)
    events_by_date: List[EventsByDate] = Field(default_factory=list)


# =========================
# Load Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    """Load prompt text from markdown file"""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_chunks(file_path: str) -> List[Dict[str, Any]]:
    """Load chunked document JSON"""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data["chunks"]


# =========================
# LLM Setup (create once)
# =========================


llm = ChatOpenAI(
    model="gpt-4.1-mini",
    api_key=os.getenv("OPENAI_API_KEY"),
    temperature=0.1,
).with_structured_output(ChunkEvents)


PROMPT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/pipeline/pass1/extract_dates.md"
)

prompt = ChatPromptTemplate.from_template(load_prompt_from_md(PROMPT_PATH))

chain = prompt | llm


# =========================
# Chunk Processor
# =========================


async def process_chunk(chunk: Dict[str, Any]) -> ChunkTimeline:
    """Extract events from a single chunk"""

    response: ChunkEvents = await chain.ainvoke({"chunk_text": chunk["suggestedText"]})

    return ChunkTimeline(
        # boundingBoxes=chunk.get("boundingBoxes", []),
        chunk=chunk.get("chunk", ""),
        chunk_number=chunk.get("chunk_number", 0),
        pageNumbers=chunk.get("pageNumbers", []),
        events_by_date=response.events_by_date,
    )


async def process_chunk_with_limit(
    chunk: Dict[str, Any], semaphore: asyncio.Semaphore
) -> Optional[ChunkTimeline]:
    """Run chunk processing under a shared semaphore limit."""

    async with semaphore:
        try:
            return await process_chunk(chunk)

        except Exception as e:
            print(f"Chunk failed: {chunk.get('chunk')} -> {e}")
            return None


async def process_document(file_path: str) -> List[ChunkTimeline]:
    """Process the document"""

    chunks = load_chunks(file_path)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHUNK_CALLS)

    tasks = [process_chunk_with_limit(chunk, semaphore) for chunk in chunks]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    clean_results = []

    for r in results:
        if isinstance(r, Exception):
            print("Task failed:", r)
        elif r:
            clean_results.append(r)

    return clean_results


if __name__ == "__main__":

    file_path = "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input3_pass0.json"
    start_time = time()
    results = asyncio.run(process_document(file_path))
    end_time = time()
    print(f"Time taken: {end_time - start_time} seconds")
    with open(
        "/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/input3_pass1.json",
        "w",
        encoding="utf-8",
    ) as f:
        f.write(
            json.dumps([r.model_dump() for r in results], indent=2, ensure_ascii=False)
        )

    print(f"Extracted {len(results)} dates from {file_path}")
