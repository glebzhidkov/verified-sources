"""Helpers for the Kafka DLT source."""

import dlt
import json
from typing import Optional

# from kafka import KafkaConsumer
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from dlt.common.configuration.specs import BaseConfiguration, configspec


@configspec
class KafkaSourceConfiguration(BaseConfiguration):
    bootstrap_servers: str
    group_id: str
    sasl_mechanisms: Optional[str] = 'PLAIN'
    security_protocol: Optional[str] = 'SASL_SSL'
    sasl_username: Optional[str]
    sasl_password: Optional[str]


def consumer_from_credentials(
        bootstrap_servers: str,
        group_id: Optional[str],
        sasl_mechanisms: Optional[str],
        security_protocol: Optional[str],
        sasl_username: Optional[str],
        sasl_password: Optional[str],
) -> Consumer:
    """Create a kafka client from credentials."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'sasl.mechanisms': sasl_mechanisms,
        'security.protocol': security_protocol,
        'sasl.username': sasl_username,
        'sasl.password': sasl_password
    }
    return Consumer(config)


def get_topic_partitions(
        consumer: Consumer,
        topic: str,
        partitions: int,
        parse_json: bool = False,
        incremental: Optional[dlt.sources.incremental] = None,
):
    """Get all documents from a kafka topic."""

    if incremental is None:
        time_ago = OFFSET_BEGINNING
    else:
        time_ago = incremental.last_value

    topic_partitions = []
    for partition in range(partitions):
        topic_partitions.append(TopicPartition(topic, partition, time_ago))
    consumer.assign(topic_partitions)

    for msg in consumer.consume():
        if msg is None:
            continue
        if parse_json:
            yield json.loads(msg.value())
        else:
            yield msg.value()