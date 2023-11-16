"""Helpers for the Kafka DLT source."""

from typing import Optional


# from kafka import KafkaConsumer
from confluent_kafka import Consumer
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


