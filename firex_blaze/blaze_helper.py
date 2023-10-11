"""
    Utility functions for the firex_blaze package.
"""
import os
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
import json

from celery.app.base import Celery
from kafka.consumer.fetcher import ConsumerRecord

from firexapp.broker_manager.broker_factory import RedisManager
from firex_blaze.fast_blaze_helper import get_blaze_dir


KAFKA_EVENTS_FILE_DELIMITER = '--END_OF_EVENT--'


@dataclass
class BlazeSenderConfig:
    kafka_topic: str
    kafka_bootstrap_servers: list[str]
    max_kafka_connection_retries: int
    security_protocol: str = 'PLAINTEXT'
    ssl_cafile: str = None
    ssl_certfile: str = None
    ssl_keyfile: str = None
    ssl_password: str = None


TASK_EVENT_TO_STATE = {
    'task-sent': 'PENDING',
    'task-received': 'RECEIVED',
    'task-started': 'STARTED',
    'task-started-info': 'STARTED',
    'task-failed': 'FAILURE',
    'task-retried': 'RETRY',
    'task-succeeded': 'SUCCESS',
    'task-revoked': 'REVOKED',
    'task-rejected': 'REJECTED',
}


def get_blaze_events_file(logs_dir, instance_name=None):
    return os.path.join(get_blaze_dir(logs_dir, instance_name), 'kafka_events.json')


def get_kafka_events(logs_dir, instance_name=None):
    events = []
    for event in Path(get_blaze_events_file(logs_dir, instance_name)).read_text().split(sep=KAFKA_EVENTS_FILE_DELIMITER):
        if event:
            events.append(json.loads(event))
    return events


def aggregate_blaze_kafka_msgs(firex_id, kafka_msgs):
    from firexapp.events.event_aggregator import FireXEventAggregator
    event_aggregator = FireXEventAggregator()
    for kafka_msg in kafka_msgs:
        kafka_event = kafka_msg.value if isinstance(kafka_msg, ConsumerRecord) else kafka_msg
        if kafka_event['FIREX_ID'] == firex_id:
            inner_event = kafka_event['EVENTS'][0]
            celery_event = dict(inner_event['DATA'])
            celery_event['uuid'] = inner_event['UUID']
            event_aggregator.aggregate_events([celery_event])

    return event_aggregator.tasks_by_uuid


def celery_app_from_logs_dir(logs_dir):
    return Celery(broker=RedisManager.get_broker_url_from_logs_dir(logs_dir),
                  accept_content=['pickle', 'json'])
