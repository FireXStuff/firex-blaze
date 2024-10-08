import os
from psutil import Process, TimeoutExpired
import subprocess
import time

from firexapp.submit.install_configs import FireXInstallConfigs
from firexapp.submit.submit import OptionalBoolean
from firexapp.submit.tracking_service import TrackingService
from firexapp.common import qualify_firex_bin, select_env_vars
from firexapp.submit.console import setup_console_logging

from firex_blaze.fast_blaze_helper import get_blaze_dir
from firexapp.discovery import PkgVersionInfo


logger = setup_console_logging(__name__)


class FireXBlazeLauncher(TrackingService):

    instance_name = 'blaze'

    def __init__(self):
        self.broker_recv_ready_file = None
        self.is_ready_for_tasks = False
        self.stdout_file = None
        self.start_time = None

    def extra_cli_arguments(self, arg_parser):

        arg_parser.add_argument('--disable_blaze', '-disable_blaze',
                                help='Disable blaze data collection', default=None, const=True, nargs='?',
                                action=OptionalBoolean)

        arg_parser.add_argument('--blaze_logs_url',
                                help='Server URL from which logs can be fetched.',
                                default=None)

        # TODO: consider sensible default values, or not launching subprocess when these required args aren't supplied.
        arg_parser.add_argument('--blaze_kafka_topic',
                                help="Topic used for Blaze's Kafka bus",
                                default=None)

        arg_parser.add_argument('--blaze_bootstrap_servers',
                                help='Comma separated list of Kafka bootrap servers.',
                                default=None)

        arg_parser.add_argument('--blaze_security_protocol',
                                help='Protocol used to communicate with brokers. '
                                     'Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.',
                                default='PLAINTEXT')
        arg_parser.add_argument('--blaze_ssl_cafile',
                                help='Optional filename of ca file to use in certificate veriication.')
        arg_parser.add_argument('--blaze_ssl_certfile',
                                help='Optional filename of file in pem format containing the client certificate, as'
                                     'well as any ca certificates needed to establish the certificate’s authenticity.')
        arg_parser.add_argument('--blaze_ssl_keyfile',
                                help='Optional filename containing the client private key.')
        arg_parser.add_argument('--blaze_ssl_password',
                                help='Optional password to be used when loading the certificate chain.')

    @classmethod
    def _create_blaze_command(cls, uid, args, broker_recv_ready_file):
        cmd = [qualify_firex_bin("firex_blaze"),
               "--uid", str(uid),
               "--firex_requester", uid.firex_requester,
               "--logs_dir", uid.logs_dir,
               "--broker_recv_ready_file", broker_recv_ready_file,
               '--logs_url', uid.logs_url,
               '--kafka_topic', args.blaze_kafka_topic,
               '--bootstrap_servers', args.blaze_bootstrap_servers,
               '--instance_name', cls.instance_name,
               '--security_protocol', args.blaze_security_protocol]
        if args.blaze_ssl_cafile:
            cmd += ['--ssl_cafile', args.blaze_ssl_cafile]
        if args.blaze_ssl_certfile:
            cmd += ['--ssl_certfile', args.blaze_ssl_certfile]
        if args.blaze_ssl_keyfile:
            cmd += ['--ssl_keyfile', args.blaze_ssl_keyfile]
        if args.blaze_ssl_password:
            cmd += ['--ssl_password', args.blaze_ssl_password]
        return cmd

    def start(self, args, install_configs: FireXInstallConfigs, uid=None, **kwargs) -> {}:
        super().start(args, install_configs, uid=uid, **kwargs)
        sufficient_args = uid.logs_url and args.blaze_kafka_topic and args.blaze_bootstrap_servers
        if args.disable_blaze or not sufficient_args:
            if args.disable_blaze:
                logger.debug("Blaze disabled; will not launch subprocess.")
            if not sufficient_args:
                logger.warning("Blaze did not receive sufficient arguments; will not launch subprocess.")
            self.is_ready_for_tasks = True
            return {}

        blaze_debug_dir = get_blaze_dir(uid.logs_dir, instance_name=self.instance_name)
        os.makedirs(blaze_debug_dir, exist_ok=True)
        self.broker_recv_ready_file = os.path.join(blaze_debug_dir, 'blaze_celery_recvr_ready')
        self.stdout_file = os.path.join(blaze_debug_dir, 'blaze.stdout')

        self.start_time = time.time()
        with open(self.stdout_file, 'w+') as f:
            pid = subprocess.Popen(
                self._create_blaze_command(uid, args, self.broker_recv_ready_file),
                stdout=f,
                stderr=subprocess.STDOUT,
                close_fds=True,
                env=select_env_vars(['PATH']),
                cwd=blaze_debug_dir,
            ).pid

        try:
            Process(pid).wait(0.1)
        except TimeoutExpired:
            logger.debug("Started background FireXBlaze with pid %s" % pid)
        else:
            logger.error("Failed to start FireXBlaze -- task data will not be put on Kafka bus.")

        return {}

    def ready_for_tasks(self, **kwargs) -> bool:
        if not self.is_ready_for_tasks:
            self.is_ready_for_tasks = os.path.isfile(self.broker_recv_ready_file)
            if self.is_ready_for_tasks:
                logger.debug("Blaze up after %.2f s" % (time.time() - self.start_time))

        return self.is_ready_for_tasks

    @staticmethod
    def get_pkg_version_info() -> PkgVersionInfo:
        import firex_blaze
        return PkgVersionInfo(pkg='firex-blaze',
                              version=firex_blaze.__version__,
                              commit=firex_blaze._version.get_versions()['full-revisionid'])