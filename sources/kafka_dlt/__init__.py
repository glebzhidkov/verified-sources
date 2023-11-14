"""Source that loads topics form kafka, supports incremental loads."""

from typing import Any, Iterable, Optional

from pendulum import DateTime
import dlt
import json
from dlt.extract.source import DltResource
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.utils import digest128

from .helpers import consumer_from_credentials, KafkaSourceConfiguration
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin._metadata import TopicMetadata


@dlt.source(name="kafka", spec=KafkaSourceConfiguration)
def kafka_source(
    bootstrap_servers: str = dlt.secrets.value,
    group_id: Optional[str] = dlt.config.value,
    sasl_mechanisms: Optional[str] = dlt.config.value,
    security_protocol: Optional[str] = dlt.config.value,
    sasl_username: Optional[str] = dlt.config.value,
    sasl_password: Optional[str] = dlt.config.value,
) -> Iterable[DltResource]:
    """
    A DLT source which loads data from a kafka database using python kafka."""

    # set up kafka client
    consumer = consumer_from_credentials(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        sasl_mechanisms=sasl_mechanisms,
        security_protocol=security_protocol,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    )

    topic_list = consumer.list_topics()
    for topic_name, partitions in topic_list.topics.items():
        yield dlt.resource(  # type: ignore
            get_topic_messages,
            name=topic_name,
        )(consumer, topic_name, partitions)


def get_topic_messages(
    consumer: Consumer,
    topic: str,
    partitions: TopicMetadata,
    parse_json: bool = False,
    incremental: Optional[dlt.sources.incremental[DateTime]] = None,
) -> Iterable[dict]:
    """Get all documents from a kafka topic."""

    if incremental is None:
        time_ago = OFFSET_BEGINNING
    else:
        time_ago = incremental.last_value
    
    topic_partitions = []
    for topic_partition in partitions.partitions:
        topic_partitions.append(TopicPartition(topic, topic_partition, offset=time_ago))

    consumer.assign(topic_partitions)
    last_offsets = {t.partition: consumer.get_watermark_offsets(t)[1] for t in topic_partitions}
    # Consume the messages
    while True:
        response = consumer.consume(num_messages=1000, timeout=5)
        if response:
            records = []
            for record in response:
                offset = record.offset()
                content = record.value()
                ts_rec = record.timestamp()
                # if timestamp is not available, set it to None
                timestamp = ts_rec[1] if ts_rec[0] else None
                message = {
                    "_kinesis": {
                        "partition_id": record.partition(),
                        "offset": offset,
                        "ts": timestamp,
                        "partition": record["PartitionKey"],
                        "topic_name": topic,
                    },
                    "_kinesis_msg_id": digest128(topic + offset),
                }
                if parse_json:
                    message.update(json.loadb(content))
                else:
                    message["data"] = content
                records.append(message)
            yield records

        positions = consumer.position(topic_partitions)
        if all(p.offset >= last_offsets[p.partition] for p in positions):
            break
