from typing import List

import dlt
from dlt.common import pendulum
from dlt.pipeline.pipeline import Pipeline
from dlt.common.pipeline import LoadInfo

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .kafka_dlt import kafka_source  # type: ignore
except ImportError:
    from kafka_dlt import kafka_source


def load_select_topics() -> None:
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_kafka",
            destination="duckdb",
            dataset_name="kafka_select",
        )

    # Configure the source to load a few select topics incrementally
    items = kafka_source(incremental=dlt.sources.incremental("date")).with_resources("items")

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(items, write_disposition="merge")
    print(info)


def load_select_collection_db_filtered() -> None:

    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_kafka",
            destination="duckdb",
            dataset_name="kafka_select_incremental",
        )

    # Configure the source to load a few select topics incrementally
    items = kafka_source(incremental=dlt.sources.incremental(initial_value=pendulum.DateTime(2016, 1, 1, 0, 0, 0)),)

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(items, write_disposition="merge")
    print(info)

def load_entire_database(pipeline: Pipeline = None) -> LoadInfo:
    """Use the kafka source to completely load all collection in a database"""
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="local_kafka",
            destination="duckdb",
            dataset_name="kafka_database",
        )

    # By default the kafka source reflects all collections in the database
    source = kafka_source()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")

    print(info)


def load_kafka() -> LoadInfo:
    """Use the kafka source to completely load all collection in a database"""

    # By default the kafka source reflects all collections in the database
    source = kafka_source(group_id="PythonProducer")

    values = list(source)
    values


if __name__ == "__main__":
    # load_entire_database()
    load_kafka()
