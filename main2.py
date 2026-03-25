import asyncio
from pathlib import Path
from loguru import logger

from workflow2.graph.executor import run_fraud_timeline_workflow2


xray_dir = Path(
    "/home/ankit/smartsense_code/fraudx_timeline_poc/download_files/xray_jsons2"
)
input_files = sorted(str(p) for p in xray_dir.glob("*-xray.json"))
Path("logs").mkdir(parents=True, exist_ok=True)

logger.add("logs/workflow2_{time}.log", level="INFO", rotation="50 MB")

final_state = asyncio.run(
    run_fraud_timeline_workflow2(
        input_raw_files=input_files,
        run_label="all_xray_run_workflow2_002_10",
        output_dir="/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons",
        prompt_step4_path=(
            "/home/ankit/smartsense_code/fraudx_timeline_poc/"
            "new_pipe/final_timeline/prompt.md"
        ),
        prompt_step5_1_path=(
            "/home/ankit/smartsense_code/fraudx_timeline_poc/"
            "workflow2/prompts/conflict_prompt1.md"
        ),
        prompt_step5_2_path=(
            "/home/ankit/smartsense_code/fraudx_timeline_poc/"
            "workflow2/prompts/conflict_prompt2.md"
        ),
        step3_output_filename="combined_pass1_by_date.json",
        step4_output_filename="pass2_output.json",
        step5_output_filename="conflicts.json",
        step4_max_concurrent=40,
        step5_group_size=10,
        step5_max_concurrent=40,
    )
)

print("Step5 output:", final_state.get("step5_output"))
print("Conflict count:", final_state.get("step5_conflict_count", 0))
