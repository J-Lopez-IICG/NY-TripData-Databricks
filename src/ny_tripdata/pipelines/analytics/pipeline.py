from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_gold_fact_trips


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_gold_fact_trips,
                inputs=None,
                outputs="mensaje_gold_fact",
                name="node_gold_fact_trips",
            ),
        ]
    )
