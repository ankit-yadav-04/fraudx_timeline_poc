"""Simple QA over GroundX chunks using an LLM prompt."""

import asyncio
import os
from typing import Any, List

from dotenv import load_dotenv
from groundx import AsyncGroundX
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field


load_dotenv()


# =========================
# Config
# =========================

DEFAULT_MODEL = "gpt-4.1-mini"
DEFAULT_TOP_K = 10
DEFAULT_VERBOSITY = 2
DEFAULT_PROMPT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/qa.md"
)


# =========================
# Pydantic Schemas
# =========================


class QAResponse(BaseModel):
    answer: str = Field(
        description="Final answer to the question based only on provided chunks. "
        'If not found, return exactly "NOT FOUND".'
    )


# =========================
# Helpers
# =========================


def load_prompt_from_md(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def build_chain(prompt_path: str, model_name: str):
    llm = ChatOpenAI(
        model=model_name,
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
    ).with_structured_output(QAResponse)

    prompt = ChatPromptTemplate.from_template(load_prompt_from_md(prompt_path))
    return prompt | llm


def _extract_chunk_text(result: dict[str, Any]) -> str:
    """
    Prefer 'suggested_text' from GroundX; fallback to 'text'.
    """
    suggested = result.get("suggested_text")
    if isinstance(suggested, str) and suggested.strip():
        return suggested.strip()

    text = result.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()

    return ""


def format_chunks_for_prompt(chunks: List[str]) -> str:
    """
    Build readable chunk block for prompt variable {chunks}.
    """
    lines: List[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        lines.append(f"[CHUNK {idx}]")
        lines.append(chunk)
        lines.append("")
    return "\n".join(lines).strip()


# =========================
# GroundX Retrieval
# =========================


async def fetch_chunks_from_groundx(
    client: AsyncGroundX,
    bucket_id: int,
    query: str,
    top_k: int = DEFAULT_TOP_K,
    verbosity: int = DEFAULT_VERBOSITY,
) -> List[str]:
    search_response = await client.search.content(
        id=bucket_id,
        query=query,
        n=top_k,
        verbosity=verbosity,
    )
    payload = search_response.model_dump()

    raw_results = payload.get("search", {}).get("results", [])
    if not isinstance(raw_results, list):
        return []

    chunks: List[str] = []
    for row in raw_results:
        if not isinstance(row, dict):
            continue
        chunk_text = _extract_chunk_text(row)
        if chunk_text:
            chunks.append(chunk_text)

    return chunks


# =========================
# Main QA Flow
# =========================


async def answer_question_from_bucket(
    question: str,
    bucket_id: int,
    prompt_path: str = DEFAULT_PROMPT_PATH,
    model_name: str = DEFAULT_MODEL,
    top_k: int = DEFAULT_TOP_K,
    verbosity: int = DEFAULT_VERBOSITY,
) -> dict[str, Any]:
    if not question.strip():
        raise ValueError("question cannot be empty")

    groundx_api_key = os.getenv("GROUNDX_API_KEY")
    openai_api_key = os.getenv("OPENAI_API_KEY")

    if not groundx_api_key:
        raise ValueError("GROUNDX_API_KEY is not set")
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY is not set")

    client = AsyncGroundX(api_key=groundx_api_key)
    chain = build_chain(prompt_path=prompt_path, model_name=model_name)

    chunks = await fetch_chunks_from_groundx(
        client=client,
        bucket_id=bucket_id,
        query=question,
        top_k=top_k,
        verbosity=verbosity,
    )

    chunks_block = format_chunks_for_prompt(chunks)
    response: QAResponse = await chain.ainvoke(
        {
            "question": question,
            "chunks": chunks_block,
        }
    )

    return {
        "question": question,
        "bucket_id": bucket_id,
        "chunks_used": len(chunks),
        "answer": response.answer.strip(),
    }


# =========================
# Example Run
# =========================

if __name__ == "__main__":
    # Update these two values for your run
    TEST_BUCKET_ID = 22997
    TEST_QUESTION = "what was the cause of the accident or incident?"

    result = asyncio.run(
        answer_question_from_bucket(
            question=TEST_QUESTION,
            bucket_id=TEST_BUCKET_ID,
        )
    )

    print("Question:", result["question"])
    print("Chunks used:", result["chunks_used"])
    print("Answer:", result["answer"])
