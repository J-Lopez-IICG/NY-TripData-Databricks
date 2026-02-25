from typing import Dict
from kedro.pipeline import Pipeline
from ny_tripdata.pipelines import data_processing as dp
from ny_tripdata.pipelines import analytics as an


def register_pipelines() -> Dict[str, Pipeline]:
    processing_pipe = dp.create_pipeline()
    analytics_pipe = an.create_pipeline()

    return {
        "__default__": processing_pipe + analytics_pipe,
        "dp": processing_pipe,
        "an": analytics_pipe,
    }
