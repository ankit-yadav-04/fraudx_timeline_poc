"""Executor helpers for running workflow2 fraud timeline graph."""

import time
import uuid
from pathlib import Path

import warnings
from dotenv import load_dotenv
from langfuse.langchain import CallbackHandler
from loguru import logger

from workflow2.graph.builder import FraudTimelineWorkflow2Builder


langfuse_handler = CallbackHandler()

load_dotenv()

warnings.filterwarnings("ignore", message=".*PydanticSerializationUnexpectedValue.*")


async def run_fraud_timeline_workflow2(
    input_raw_files: list[str],
    run_label: str | None = None,
    output_dir: str = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons",
    prompt_step2_path: str | None = None,
    prompt_step4_path: str | None = None,
    prompt_step5_1_path: str | None = None,
    prompt_step5_2_path: str | None = None,
    step3_output_filename: str = "combined_pass1_by_date.json",
    step4_output_filename: str = "pass2_output.json",
    step5_output_filename: str = "conflicts.json",
    step4_max_concurrent: int = 20,
    step5_group_size: int = 10,
    step5_max_concurrent: int = 10,
) -> dict:
    """
    Run workflow2 end-to-end and return final state.

    Mirrors workflow/graph/executor.py style with explicit input validation.
    """
    start_time = time.time()
    logger.info(f"Starting workflow2 at {start_time}")
    if not input_raw_files:
        raise ValueError("input_raw_files is required and cannot be empty.")

    for file_path in input_raw_files:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Input file does not exist: {file_path}")

    builder = FraudTimelineWorkflow2Builder()
    app = await builder.build()

    config = {
        "configurable": {
            "thread_id": str(uuid.uuid4()),
        },
        "callbacks": [langfuse_handler],
    }

    initial_state = {
        "input_raw_files": input_raw_files,
        "run_label": run_label or f"run_{uuid.uuid4().hex[:8]}",
        "output_dir": output_dir,
        "prompt_step2_path": prompt_step2_path,
        "prompt_step4_path": prompt_step4_path,
        "prompt_step5_1_path": prompt_step5_1_path,
        "prompt_step5_2_path": prompt_step5_2_path,
        "step3_output_filename": step3_output_filename,
        "step4_output_filename": step4_output_filename,
        "step5_output_filename": step5_output_filename,
        "step4_max_concurrent": step4_max_concurrent,
        "step5_group_size": step5_group_size,
        "step5_max_concurrent": step5_max_concurrent,
    }

    result = await app.ainvoke(initial_state, config=config)

    end_time = time.time()
    logger.info(f"Workflow2 completed in {end_time - start_time:.2f} seconds")
    return result
