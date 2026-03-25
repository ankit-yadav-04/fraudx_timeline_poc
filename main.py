import asyncio
from pathlib import Path

from workflow.graph.executor import run_fraud_timeline_workflow

xray_dir = Path(
    "/home/ankit/smartsense_code/fraudx_timeline_poc/download_files/xray_jsons"
)
input_files = sorted(str(p) for p in xray_dir.glob("*-xray.json"))

final_state = asyncio.run(
    run_fraud_timeline_workflow(
        input_chunk_files=input_files,
        run_label="all_xray_run_002",
        output_dir="/home/ankit/smartsense_code/fraudx_timeline_poc/workflow/jsons",
    )
)

print("Final output:", final_state["pass5_output"])
