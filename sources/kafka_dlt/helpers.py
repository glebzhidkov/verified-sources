"""Helpers for the Kafka DLT source."""

import dlt
from typing import Optional

from kafka import KafkaConsumer

def consumer_from_credentials(
    bootstrap_servers: str = dlt.secrets.value,
    group_id: Optional[str] = dlt.config.value,
) -> KafkaConsumer:
    """Create a kafka client from credentials."""
    return KafkaConsumer(
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        session_timeout_ms=60000, #1minute to avoid unexpected closes
    )
