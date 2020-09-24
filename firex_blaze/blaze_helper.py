"""
    Utility functions for the firex_blaze package.
"""
import os
from collections import namedtuple
from pathlib import Path
import json

from firexapp.submit.uid import Uid
from kafka.consumer.fetcher import ConsumerRecord

KAFKA_EVENTS_FILE_DELIMITER = '--END_OF_EVENT--'


BlazeSenderConfig = namedtuple('BlazeSenderConfig', ['kafka_topic', 'kafka_bootstrap_servers'])

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


def get_blaze_dir(logs_dir, instance_name=None):
    if instance_name is None:
        instance_name = 'blaze'
    return os.path.join(logs_dir, Uid.debug_dirname, instance_name)


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
