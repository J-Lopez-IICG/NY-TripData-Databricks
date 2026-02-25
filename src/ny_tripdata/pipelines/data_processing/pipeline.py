from kedro.pipeline import Pipeline, node, pipeline
from .nodes import create_silver_table


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_silver_table,
                inputs=None,
                outputs="mensaje_silver",
                name="crear_tabla_plata_node",
            )
        ]
    )
