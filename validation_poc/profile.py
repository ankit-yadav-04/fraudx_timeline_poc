import argparse
import asyncio
import json
import os
from pathlib import Path
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
DEFAULT_BUCKET_ID = 22997
DEFAULT_MODEL = "gpt-4.1-mini"
DEFAULT_TOP_K = 10
DEFAULT_VERBOSITY = 2
DEFAULT_INPUT_JSON = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/questions.json"
)
DEFAULT_OUTPUT_JSON = "validation_poc/questions_with_answers.json"
DEFAULT_PROMPT_PATH = (
    "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/qa.md"
)

NAME_QUESTION_TEXT = "What is the full name of the patient/claimant?"
NAME_QUESTION_TYPE = "Identity"
MAX_CONCURRENT_QA_CALLS = 4  # how many questions answered in parallel


# =========================
# Pydantic Schema
# =========================
class QAResponse(BaseModel):
    answer: str = Field(
        description=(
            "Final answer to the question based only on provided chunks. "
            'If not found, return exactly "NOT FOUND".'
        )
    )


# =========================
# Prompt + LLM Chain
# =========================
def load_prompt_from_md(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def build_chain(prompt_path: str, model_name: str):
    llm = ChatOpenAI(
        model=model_name,
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
    ).with_structured_output(QAResponse)

    prompt = ChatPromptTemplate.from_template(load_prompt_from_md(prompt_path))
    return prompt | llm


# =========================
# GroundX Retrieval Helpers
# =========================
def _extract_chunk_text(result: dict[str, Any]) -> str:
    suggested = result.get("suggested_text")
    if isinstance(suggested, str) and suggested.strip():
        return suggested.strip()

    text = result.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()

    return ""


def format_chunks_for_prompt(chunks: List[str]) -> str:
    lines: List[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        lines.append(f"[CHUNK {idx}]")
        lines.append(chunk)
        lines.append("")
    return "\n".join(lines).strip()


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
        text = _extract_chunk_text(row)
        if text:
            chunks.append(text)
    return chunks


# =========================
# QA Core (Standalone)
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
# JSON + Name Utilities
# =========================
def load_questions(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    questions = payload.get("questions", [])
    if not isinstance(questions, list):
        raise ValueError("Invalid format: 'questions' must be a list")
    return questions


def save_answers(path: Path, questions_with_answers: list[dict[str, Any]]) -> None:
    output = {"questions": questions_with_answers}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")


def ensure_name_question(questions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Ensures name question exists in the JSON list.
    If missing, inserts it at question_number=1 and re-numbers all questions.
    """
    found = any(
        isinstance(q.get("question"), str)
        and q.get("question").strip().lower() == NAME_QUESTION_TEXT.strip().lower()
        for q in questions
    )
    if found:
        return questions

    injected = {
        "question_number": 1,
        "question_type": NAME_QUESTION_TYPE,
        "question": NAME_QUESTION_TEXT,
    }
    updated = [injected] + questions

    # Re-number cleanly
    for i, q in enumerate(updated, start=1):
        q["question_number"] = i
    return updated


def normalize_name(value: str) -> str:
    text = (value or "").strip()
    if not text or text.upper() == "NOT FOUND" or text.startswith("ERROR:"):
        return ""
    return text


def resolve_question(question: str, patient_name: str) -> str:
    """
    Replaces {patient_name} placeholder with actual patient name.
    Falls back to 'the patient' if name is not available.
    """
    q = question.strip()
    if not patient_name:
        return q.replace("{patient_name}", "the patient")
    return q.format(patient_name=patient_name)


# =========================
# Main Batch Flow
# =========================
async def answer_single_question_with_limit(
    item: dict[str, Any],
    patient_name: str,
    bucket_id: int,
    prompt_path: str,
    model_name: str,
    top_k: int,
    verbosity: int,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    q_num = item.get("question_number")
    q_type = item.get("question_type")
    question = (item.get("question") or "").strip()

    if not question:
        return {
            "question_number": q_num,
            "question_type": q_type,
            "question": question,
            "resolved_question": question,
            "answer": "ERROR: Empty question",
        }

    resolved_question = resolve_question(question, patient_name)

    async with semaphore:
        try:
            qa_result = await answer_question_from_bucket(
                question=resolved_question,
                bucket_id=bucket_id,
                prompt_path=prompt_path,
                model_name=model_name,
                top_k=top_k,
                verbosity=verbosity,
            )
            answer = qa_result.get("answer", "NOT FOUND")
        except Exception as exc:
            answer = f"ERROR: {exc}"

    print(f"Done Q{q_num}: {question[:80]}")
    return {
        "question_number": q_num,
        "question_type": q_type,
        "question": question,
        "resolved_question": resolved_question,
        "answer": answer,
    }


async def answer_all_questions(
    questions: list[dict[str, Any]],
    bucket_id: int,
    prompt_path: str,
    model_name: str,
    top_k: int,
    verbosity: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    # Pass 1: ensure name question and answer it first
    questions = ensure_name_question(questions)
    name_item = next(
        (
            q
            for q in questions
            if q.get("question", "").strip().lower() == NAME_QUESTION_TEXT.lower()
        ),
        questions[0],
    )

    patient_name = ""
    try:
        name_question = (name_item.get("question") or "").strip()
        name_result = await answer_question_from_bucket(
            question=name_question,
            bucket_id=bucket_id,
            prompt_path=prompt_path,
            model_name=model_name,
            top_k=top_k,
            verbosity=verbosity,
        )
        patient_name = normalize_name(name_result.get("answer", ""))
        results.append(
            {
                "question_number": name_item.get("question_number"),
                "question_type": name_item.get("question_type"),
                "question": name_question,
                "resolved_question": name_question,
                "answer": name_result.get("answer", "NOT FOUND"),
            }
        )
        print(f"Done Q{name_item.get('question_number')}: {name_question[:80]}")
    except Exception as exc:
        results.append(
            {
                "question_number": name_item.get("question_number"),
                "question_type": name_item.get("question_type"),
                "question": name_item.get("question", ""),
                "resolved_question": name_item.get("question", ""),
                "answer": f"ERROR: {exc}",
            }
        )

    # Pass 2: answer remaining in parallel with concurrency limit
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_QA_CALLS)

    remaining_questions = [q for q in questions if q is not name_item]

    tasks = [
        answer_single_question_with_limit(
            item=item,
            patient_name=patient_name,
            bucket_id=bucket_id,
            prompt_path=prompt_path,
            model_name=model_name,
            top_k=top_k,
            verbosity=verbosity,
            semaphore=semaphore,
        )
        for item in remaining_questions
    ]

    parallel_results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in parallel_results:
        if isinstance(r, Exception):
            print(f"Task failed with exception: {r}")
            continue
        results.append(r)

    # Keep output sorted by question_number
    results.sort(key=lambda x: int(x.get("question_number", 999999)))
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run standalone QA for all questions in JSON"
    )
    parser.add_argument(
        "--input", default=DEFAULT_INPUT_JSON, help="Input questions JSON path"
    )
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT_JSON, help="Output answers JSON path"
    )
    parser.add_argument(
        "--bucket-id",
        type=int,
        default=DEFAULT_BUCKET_ID,
        help=f"GroundX bucket ID (default: {DEFAULT_BUCKET_ID})",
    )
    parser.add_argument(
        "--prompt-path", default=DEFAULT_PROMPT_PATH, help="Prompt markdown path"
    )
    parser.add_argument("--model-name", default=DEFAULT_MODEL, help="OpenAI model name")
    parser.add_argument(
        "--top-k", type=int, default=DEFAULT_TOP_K, help="Chunks to retrieve"
    )
    parser.add_argument(
        "--verbosity", type=int, default=DEFAULT_VERBOSITY, help="GroundX verbosity"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    questions = load_questions(input_path)
    answered = asyncio.run(
        answer_all_questions(
            questions=questions,
            bucket_id=args.bucket_id,
            prompt_path=args.prompt_path,
            model_name=args.model_name,
            top_k=args.top_k,
            verbosity=args.verbosity,
        )
    )
    save_answers(output_path, answered)
    print(f"Saved {len(answered)} answers to: {output_path}")


if __name__ == "__main__":
    main()
