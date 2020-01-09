"""
Process events from Celery and put them on a kafka bus.
"""

import logging
import json
import os

from kafka import KafkaProducer
from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.model import FireXRunMetadata, COMPLETE_RUNSTATES

from firex_blaze.blaze_helper import get_blaze_dir, BlazeSenderConfig, TASK_EVENT_TO_STATE


logger = logging.getLogger(__name__)


SEND_EVENT_TYPES = ('task-succeeded', 'task-failed', 'task-revoked', 'task-started', 'task-completed', 'task-results')


class KafkaSenderThread(BrokerEventConsumerThread):
    """Captures Celery events and puts them on a Kafka bus."""

    def __init__(self,
                 celery_app,
                 run_metadata: FireXRunMetadata,
                 config: BlazeSenderConfig,
                 logs_url: str,
                 max_retry_attempts: int = None,
                 receiver_ready_file: str = None):

        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)
        self.firex_id = run_metadata.firex_id
        self.logs_url = logs_url
        self.kafka_topic = config.kafka_topic
        self.recording_file = os.path.join(get_blaze_dir(run_metadata.logs_dir), 'kafka_events.json')

        self.producer = KafkaProducer(bootstrap_servers=config.kafka_bootstrap_servers)
        self.uuid_to_task_name_mapping = {}
        self.root_task = {'uuid': None, 'is_complete': False}

    def _is_root_complete(self):
        return self.root_task['is_complete']

    def _update_root_task(self, event):
        if (event.get('type') == 'task-received'
                and 'root_id' in event
                and self.root_task['uuid'] is None):
            self.root_task['uuid'] = event['root_id']

        if event['uuid'] == self.root_task['uuid']:
            self.root_task['is_complete'] = event.get('type') in COMPLETE_RUNSTATES

    def _send_to_kafka(self, event):
        logger.info('Sending %s (%s):%s to kafka' % (event['EVENTS'][0]['UUID'],
                                                     event['EVENTS'][0]['DATA'].get('name'),
                                                     event['EVENTS'][0]['DATA'].get('type')))

        self.producer.send(self.kafka_topic, key=self.firex_id.encode('ascii'), value=json.dumps(event).encode('ascii'))

    def _get_kafka_event(self, event):
        uuid = event.pop('uuid')

        if uuid not in self.uuid_to_task_name_mapping and 'long_name' in event:
            self.uuid_to_task_name_mapping[uuid] = event['long_name']

        if uuid in self.uuid_to_task_name_mapping:
            event['name'] = self.uuid_to_task_name_mapping[uuid]

        # Not all types map to states (e.g. task-results), so only populate state for some event types.
        if event.get('type') in TASK_EVENT_TO_STATE:
            event['state'] = TASK_EVENT_TO_STATE[event.get('type')]

        # Remove result since we only should report firex_result, not the native result
        event.pop('result', None)

        return {'FIREX_ID': self.firex_id, 'LOGS_URL': self.logs_url, 'EVENTS': [{'DATA': event, 'UUID': uuid}]}

    def _on_celery_event(self, event):
        if 'uuid' not in event:
            return

        self._update_root_task(event)

        if event.get('type') in SEND_EVENT_TYPES:
            kafka_event = self._get_kafka_event(event)
            self._send_to_kafka(kafka_event)

            # Append the event to the recording file.
            with open(self.recording_file, "a") as rec:
                rec.write(json.dumps(kafka_event, sort_keys=True, indent=2) + "\n")

    def _on_cleanup(self):
        self.producer.flush()
        self.producer.close()
