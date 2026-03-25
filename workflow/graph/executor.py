"""Executor helpers for running the fraud timeline LangGraph workflow."""

import os

from dotenv import load_dotenv
import uuid
from pathlib import Path

import time
from loguru import logger
from langfuse.langchain import CallbackHandler
from workflow.graph.builder import FraudTimelineWorkflowBuilder

load_dotenv()

langfuse_handler = CallbackHandler()


async def run_fraud_timeline_workflow(
    input_chunk_files: list[str],
    run_label: str | None = None,
    output_dir: str = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow/jsons",
) -> dict:
    """Run workflow end-to-end and return final state."""
    logger.info("Pipeline started")
    start_time = time.time()
    if not input_chunk_files:
        raise ValueError("input_chunk_files is required and cannot be empty.")

    for file_path in input_chunk_files:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Input file does not exist: {file_path}")

    logger.info("Building workflow")
    builder = FraudTimelineWorkflowBuilder()
    app = await builder.build()
    logger.info("Workflow built")
    config = {
        "configurable": {"thread_id": str(uuid.uuid4())},
        "callbacks": [langfuse_handler],
    }
    initial_state = {
        "input_chunk_files": input_chunk_files,
        "run_label": run_label or f"run_{uuid.uuid4().hex[:8]}",
        "output_dir": output_dir,
    }
    logger.info("Invoking workflow")
    result = await app.ainvoke(initial_state, config=config)
    end_time = time.time()
    logger.info(f"Pipeline completed in {end_time - start_time} seconds")
    return result
