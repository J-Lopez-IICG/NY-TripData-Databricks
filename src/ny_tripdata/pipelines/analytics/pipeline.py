from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_gold_vendor_efficiency


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_gold_vendor_efficiency,
                inputs=None,
                outputs="mensaje_analytics_vendor",
                name="node_analytics_vendor_efficiency",
            ),
        ]
    )
