"""LangGraph builder for the fraud timeline workflow."""

from langgraph.graph import END, START, StateGraph

from workflow.graph.nodes import (
    merge_pass2_5_node,
    pass1_extract_dates_node,
    pass2_5_clean_doc_node,
    pass2_document_timeline_node,
    pass3_complete_timeline_node,
    pass4_compress_node,
    pass5_detect_contradictions_node,
)
from workflow.graph.state import FraudTimelineWorkflowState


class FraudTimelineWorkflowBuilder:
    """Builder responsible for compiling workflow graph."""

    async def build(self):
        """Compile and return graph application."""
        builder = StateGraph(FraudTimelineWorkflowState)

        builder.add_node("pass1_extract_dates", pass1_extract_dates_node)
        builder.add_node("pass2_document_timeline", pass2_document_timeline_node)
        builder.add_node("pass2_5_clean_doc", pass2_5_clean_doc_node)
        builder.add_node("merge_pass2_5", merge_pass2_5_node)
        builder.add_node("pass3_complete_timeline", pass3_complete_timeline_node)
        builder.add_node("pass4_compress", pass4_compress_node)
        builder.add_node("pass5_detect_contradictions", pass5_detect_contradictions_node)

        builder.add_edge(START, "pass1_extract_dates")
        builder.add_edge("pass1_extract_dates", "pass2_document_timeline")
        builder.add_edge("pass2_document_timeline", "pass2_5_clean_doc")
        builder.add_edge("pass2_5_clean_doc", "merge_pass2_5")
        builder.add_edge("merge_pass2_5", "pass3_complete_timeline")
        builder.add_edge("pass3_complete_timeline", "pass4_compress")
        builder.add_edge("pass4_compress", "pass5_detect_contradictions")
        builder.add_edge("pass5_detect_contradictions", END)

        return builder.compile()
