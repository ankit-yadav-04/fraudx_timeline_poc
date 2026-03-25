"""State definition for the fraud timeline workflow graph."""

from pydantic import BaseModel, Field


class FraudTimelineWorkflowState(BaseModel):
    """Shared state passed through workflow graph nodes."""

    input_chunk_files: list[str] = Field(default_factory=list)
    run_label: str = "run"
    output_dir: str = (
        "/home/ankit/smartsense_code/fraudx_timeline_poc/workflow/jsons"
    )

    pass1_outputs: list[str] = Field(default_factory=list)
    pass2_outputs: list[str] = Field(default_factory=list)
    pass2_5_outputs: list[str] = Field(default_factory=list)

    merged_pass2_5_output: str | None = None
    pass3_output: str | None = None
    pass4_output: str | None = None
    pass5_output: str | None = None

    compressed_timeline_text: str | None = None
