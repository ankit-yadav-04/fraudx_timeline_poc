"""LangGraph builder for workflow2 fraud timeline pipeline."""

from langgraph.graph import END, START, StateGraph

from workflow2.graph.state import FraudTimelineWorkflow2State
from workflow2.nodes.step1 import step1_clean_chunks_node
from workflow2.nodes.step2 import step2_extract_dates_node
from workflow2.nodes.step3 import step3_combine_by_date_node
from workflow2.nodes.step4 import step4_build_timeline_node
from workflow2.nodes.step5 import step5_detect_conflicts_node


class FraudTimelineWorkflow2Builder:
    """Builder responsible for compiling workflow2 graph."""

    async def build(self):
        builder = StateGraph(FraudTimelineWorkflow2State)

        builder.add_node("step1_clean_chunks", step1_clean_chunks_node)
        builder.add_node("step2_extract_dates", step2_extract_dates_node)
        builder.add_node("step3_combine_by_date", step3_combine_by_date_node)
        builder.add_node("step4_build_timeline", step4_build_timeline_node)
        builder.add_node("step5_detect_conflicts", step5_detect_conflicts_node)

        builder.add_edge(START, "step1_clean_chunks")
        builder.add_edge("step1_clean_chunks", "step2_extract_dates")
        builder.add_edge("step2_extract_dates", "step3_combine_by_date")
        builder.add_edge("step3_combine_by_date", "step4_build_timeline")
        builder.add_edge("step4_build_timeline", "step5_detect_conflicts")
        builder.add_edge("step5_detect_conflicts", END)

        return builder.compile()
