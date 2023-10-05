# Kafka
Kafka is a distributed streaming platform. It is useful for building real-time streaming data
pipelines to get data between systems or applications. It is used for building real-time streaming
data pipelines that reliably get data between systems or applications.

## How to set up
1. Install the required packages by using the command:
    ```bash
    pip install -r requirements.txt
    ```

2. Now you can build the verified source by using the command:
    ```bash
    python3 kafka_pipeline.py
    ```

3. To ensure that everything loads as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```

For example, the pipeline_name for the above pipeline example is `local_kafka`, you can use any custom name instead.

## How to use
You can use the kafka verified source to load data from your Kafka topic to a destination of your choice.

```python
from sources.kafka import kafka
from dlt.pipeline.pipeline import Pipeline

pipeline = dlt.pipeline(
    pipeline_name="kafka_pipeline", destination="duckdb", dataset_name="kafka_select"
)

database = kafka()

pipeline.database(database, write_disposition="merge")
```

If you want to select the topics, you can do it by calling the `with_resources` method with the topic names as arguments.

```python
database = kafka().with_resources("topic1", "topic2")
```

It is also possible to set the incremental parameter to load just the new records to the destination.

```python
from dlt.sources import incremental

database = kafka(incremental=incremental("date"))
```

Or you can load a single topic using the `kafka_topic` function.

```python
from sources.kafka import kafka
from dlt.pipeline.pipeline import Pipeline

pipeline = dlt.pipeline(
    pipeline_name="kafka_pipeline", destination="duckdb", dataset_name="kafka_select"
)

topic = kafka_topic("topic_name")

pipeline.database(topic, write_disposition="merge")
```

