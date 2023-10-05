"""Source that loads topics form kafka, supports incremental loads."""

from typing import Any, Iterable, Optional

from pendulum import DateTime
import dlt
from dlt.extract.source import DltResource
from dlt.common.time import ensure_pendulum_datetime

from kafka import TopicPartition, KafkaConsumer
from .helpers import consumer_from_credentials


@dlt.source(name="kafka")
def kafka_source(
    bootstrap_servers: str = dlt.secrets.value,
    group_id: Optional[str] = dlt.config.value,
) -> Iterable[DltResource]:
    """
    A DLT source which loads data from a kafka database using python kafka."""

    # set up kafka client
    consumer = consumer_from_credentials(
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
    )

    for topic in consumer.topics():
        yield dlt.resource(  # type: ignore
            get_topic_messages,
            name=topic,
        )(consumer, topic)


@dlt.common.configuration.with_config(sections=("sources", "kafka"))
def kafka_topic(
    bootstrap_servers: str = dlt.secrets.value,
    group_id: Optional[str] = dlt.config.value,
    topic: str = dlt.config.value,
) -> Any:
    """
    A DLT source which loads a collection from a kafka database using python-kafka."""
    yield from get_topic_messages(
        bootstrap_servers,
        group_id,
        topic,
    )


def get_topic_messages(
    consumer: KafkaConsumer,
    topic: str,
    incremental: Optional[dlt.sources.incremental[DateTime]] = None,
) -> Iterable[dict]:
    """Get all documents from a kafka topic."""
    topic_partitions = []
    for partition_id in consumer.partitions_for_topic(topic):
        topic_partitions.append(TopicPartition(topic, partition_id))
    consumer.assign(topic_partitions)
    for topic_partition in topic_partitions:
        if incremental:
            last_value = incremental.last_value
            consumer.seek(topic_partition, last_value)
        consumer.seek_to_beginning(topic_partition)
        if consumer._closed:
            consumer
        for message in consumer:
            if consumer._closed:
                consumer
            message_dict = message._asdict()
            message_dict["timestamp"] = ensure_pendulum_datetime(
                message_dict["timestamp"] / 1000
            )
            yield message_dict
