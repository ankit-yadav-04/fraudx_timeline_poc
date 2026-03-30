"""State definition for workflow2 fraud timeline graph."""

from pydantic import BaseModel, Field


class FraudTimelineWorkflow2State(BaseModel):
    """Shared state passed through workflow2 graph nodes."""

    # Core run config
    input_raw_files: list[str] = Field(default_factory=list)
    run_label: str = "run"
    output_dir: str = "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow2/jsons"

    # Optional prompt/config overrides
    prompt_step2_path: str | None = None
    prompt_step2_5_path: str | None = None
    patient_profile_step2_5_path: str | None = None
    prompt_step4_path: str | None = None
    prompt_step5_1_path: str | None = None
    prompt_step5_2_path: str | None = None

    step3_output_filename: str = "combined_pass1_by_date.json"
    step4_output_filename: str = "pass2_output.json"
    step5_output_filename: str = "conflicts.json"

    step4_max_concurrent: int = 20
    step5_group_size: int = 10
    step5_max_concurrent: int = 10

    # Step artifacts
    step1_outputs: list[str] = Field(default_factory=list)
    step2_outputs: list[str] = Field(default_factory=list)
    step2_failures: list[dict] = Field(default_factory=list)

    step2_5_outputs: list[str] = Field(default_factory=list)
    step2_5_failures: list[dict] = Field(default_factory=list)
    total_keep: int = 0
    total_reject: int = 0
    total_dropped_empty: int = 0

    step3_output: str | None = None
    step4_output: str | None = None
    step5_output: str | None = None

    # Summary metrics
    step5_conflict_count: int = 0
