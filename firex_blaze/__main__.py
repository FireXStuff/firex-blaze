import argparse
import logging
import os
import sys
import signal

# Prevent dependencies from taking module loading hit of pkg_resources.
sys.modules["pkg_resources"] = type('noop', (object,), {})

from celery.app.base import Celery

from firexapp.broker_manager.broker_factory import RedisManager
from firexapp.events.model import FireXRunMetadata

from firex_blaze.blaze_event_consumer import KafkaSenderThread
from firex_blaze.blaze_helper import get_blaze_dir, BlazeSenderConfig, get_blaze_events_file

logger = logging.getLogger(__name__)


def celery_app_from_logs_dir(logs_dir):
    return Celery(broker=RedisManager.get_broker_url_from_logs_dir(logs_dir))


def _parse_blaze_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance_name", help="Name of the blaze instance")
    parser.add_argument("--logs_dir", help="Logs directory for the run to keep task data for.",
                        required=True)
    parser.add_argument("--uid", help="FireX UID for the run to keep task data for.",
                        required=True)
    parser.add_argument('--broker_recv_ready_file', help='File to create immediately before capturing celery events.',
                        default=None)
    parser.add_argument('--logs_url_base', help='Webserver used from which logs can be accessed.',
                        default=None)

    parser.add_argument('--kafka_topic', help='Topic use for the Kafka bus', required=True)
    parser.add_argument('--bootstrap_servers', help='Comma seperated list of Kafka bootrap servers.', required=True)

    return parser.parse_args()


def init_blaze():
    args = _parse_blaze_args()

    run_metadata = FireXRunMetadata(args.uid, args.logs_dir, None, None)

    blaze_dir = get_blaze_dir(run_metadata.logs_dir, args.instance_name)
    os.makedirs(blaze_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(blaze_dir, 'blaze.log'),
                        level=logging.DEBUG,
                        format='[%(asctime)s][%(levelname)s][%(name)s]: %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger('kafka.producer').setLevel(logging.INFO)
    logger.info('Starting Blaze with args: %s' % args)

    signal.signal(signal.SIGTERM, lambda _, __: sys.exit(1))

    if args.logs_url_base:
        logs_url = args.logs_url_base + args.logs_dir
    else:
        logs_url = None

    celery_app = celery_app_from_logs_dir(run_metadata.logs_dir)
    blaze_sender_config = BlazeSenderConfig(args.kafka_topic, args.bootstrap_servers.split(','))
    recording_file = get_blaze_events_file(run_metadata.logs_dir, args.instance_name)
    return celery_app, run_metadata, args.broker_recv_ready_file, blaze_sender_config, logs_url, recording_file


def main():
    celery_app, run_metadata, receiver_ready_file, blaze_sender_config, logs_url, recording_file = init_blaze()
    KafkaSenderThread(celery_app, run_metadata, blaze_sender_config, logs_url,
                      receiver_ready_file=receiver_ready_file,
                      recording_file=recording_file).run()


if __name__ == '__main__':
    main()
