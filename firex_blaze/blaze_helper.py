"""
    Utility functions for the firex_blaze package.
"""
import os
from collections import namedtuple


from firexapp.submit.uid import Uid

BlazeSenderConfig = namedtuple('BlazeSenderConfig', ['kafka_topic', 'kafka_bootstrap_servers'])

TASK_EVENT_TO_STATE = {
    'task-sent': 'PENDING',
    'task-received': 'RECEIVED',
    'task-started': 'STARTED',
    'task-failed': 'FAILURE',
    'task-retried': 'RETRY',
    'task-succeeded': 'SUCCESS',
    'task-revoked': 'REVOKED',
    'task-rejected': 'REJECTED',
}


def get_blaze_dir(logs_dir):
    return os.path.join(logs_dir, Uid.debug_dirname, 'blaze')
