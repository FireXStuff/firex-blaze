"""
Process events from Celery and put them on a kafka bus.
"""

import logging
import json
import time
from getpass import getuser

from kafka import KafkaProducer
from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.model import FireXRunMetadata, COMPLETE_RUNSTATES

from firex_blaze.blaze_helper import BlazeSenderConfig, TASK_EVENT_TO_STATE, KAFKA_EVENTS_FILE_DELIMITER

logger = logging.getLogger(__name__)

SEND_EVENT_TYPES = ('task-succeeded', 'task-failed', 'task-revoked', 'task-started-info', 'task-completed', 'task-results',
                    'task-instrumentation')


def format_kafka_message(firex_id, event_data, uuid, logs_url, submitter=getuser()):
    return {'FIREX_ID': firex_id,
            'SUBMITTER': submitter,
            'LOGS_URL': logs_url,               # Shouldn't be required, but Lumens needs it!
            'EVENTS': [{'DATA': event_data,
                        'UUID': uuid}]}


def send_kafka_mssg(kafka_producer, kafka_mssg, kafka_topic, firex_id):
    kafka_producer.send(topic=kafka_topic,
                        value=json.dumps(kafka_mssg).encode('ascii'),
                        key=firex_id.encode('ascii'))


def get_basic_event(name, event_type, timestamp=None, event_timestamp=time.time()):
    if timestamp is None:
        timestamp = event_timestamp

    event_data = {'name': name,
                  'type': event_type,
                  'timestamp': timestamp,               # Shouldn't be required, but Lumens needs it!
                  'event_timestamp': event_timestamp}

    # Not all types map to states (e.g. task-results), so only populate state for some event types.
    if event_type in TASK_EVENT_TO_STATE:
        event_data['state'] = TASK_EVENT_TO_STATE[event_type]

    return event_data


class NoNameForEvent(Exception):
    pass


class KafkaSenderThread(BrokerEventConsumerThread):
    """Captures Celery events and puts them on a Kafka bus."""

    def __init__(self,
                 celery_app,
                 run_metadata: FireXRunMetadata,
                 config: BlazeSenderConfig,
                 logs_url: str,
                 max_retry_attempts: int = None,
                 receiver_ready_file: str = None,
                 recording_file: str = None):

        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)
        self.submitter = getuser()
        self.firex_id = run_metadata.firex_id
        self.logs_url = logs_url
        self.kafka_topic = config.kafka_topic
        self.recording_file = recording_file

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

    def _get_kafka_event(self, event):
        uuid = event.pop('uuid')

        if uuid not in self.uuid_to_task_name_mapping and 'long_name' in event:
            self.uuid_to_task_name_mapping[uuid] = event['long_name']

        if uuid in self.uuid_to_task_name_mapping:
            name = self.uuid_to_task_name_mapping[uuid]
        else:
            # No need to produce this event since it won't be processed by Lumens anyways
            raise NoNameForEvent(f'No name found for {event}; can not send the event')

        # Remove result since we only should report firex_result, not the native result
        event.pop('result', None)

        # Add the event_timestamp (copy of the local_received), since the native timestamp that
        # Celery provides is broken (its local time instead of UTC, and utcoffset is inaccurate).
        # This piece of -redundant- data is just because Lumens can't make local_received query-able

        basic_event_data = get_basic_event(name=name,
                                           event_type=event.get('type'),
                                           timestamp=event['timestamp'],
                                           event_timestamp=event['local_received'])

        event.update(**basic_event_data)

        return format_kafka_message(firex_id=self.firex_id,
                                    event_data=event,
                                    uuid=uuid,
                                    logs_url=self.logs_url,
                                    submitter=self.submitter)

    def _on_celery_event(self, event):
        if 'uuid' not in event:
            return

        self._update_root_task(event)

        if event.get('type') in SEND_EVENT_TYPES:
            try:
                kafka_event = self._get_kafka_event(event)
            except NoNameForEvent as e:
                logger.exception(e)
            else:
                send_kafka_mssg(kafka_producer=self.producer,
                                kafka_mssg=kafka_event,
                                kafka_topic=self.kafka_topic,
                                firex_id=self.firex_id)

                # Append the event to the recording file.
                with open(self.recording_file, "a") as rec:
                    rec.write(json.dumps(kafka_event, sort_keys=True, indent=2) + KAFKA_EVENTS_FILE_DELIMITER)

    def _on_cleanup(self):
        self.producer.flush()
        self.producer.close()
