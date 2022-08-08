import hashlib
import logging
import uuid
from pathlib import Path

import posthog

import monitor.paths
import tracking.env
from clients.dbt.dbt_runner import DbtRunner
from config.config import Config
from utils.package import get_package_version

logging.getLogger('posthog').disabled = True


class AnonymousTracking:
    ANONYMOUS_USER_ID_FILE = '.user_id'
    POSTHOG_API_KEY = 'phc_56XBEzZmh02mGkadqLiYW51eECyYKWPyecVwkGdGUfg'
    POSTHOG_HOST = 'https://app.posthog.com'

    def __init__(self, config: Config) -> None:
        self.env_props = None
        self.anonymous_user_id = None
        self.anonymous_warehouse_id = None
        self.config = config
        self.do_not_track = config.anonymous_tracking_enabled is False
        self.run_id = str(uuid.uuid4())
        self.init()

    def init(self):
        try:
            self.env_props = tracking.env.get_props()
            self.anonymous_user_id = self.init_anonymous_user_id()
            self.anonymous_warehouse_id = self._get_anonymous_warehouse_id()
            posthog.api_key, posthog.host = self.POSTHOG_API_KEY, self.POSTHOG_HOST
        except Exception:
            pass

    def init_anonymous_user_id(self):
        legacy_user_id_path = Path().joinpath(self.config.profiles_dir, self.ANONYMOUS_USER_ID_FILE)
        user_id_path = Path().joinpath(self.config.config_dir, self.ANONYMOUS_USER_ID_FILE)
        # First check legacy file path
        try:
            return legacy_user_id_path.read_text()
        except OSError:
            pass
        try:
            return user_id_path.read_text()
        except OSError:
            pass
        user_id = str(uuid.uuid4())
        try:
            user_id_path.write_text(user_id)
        except OSError:
            pass
        return user_id

    def send_event(self, name: str, properties: dict = None) -> None:
        if self.do_not_track:
            return

        if properties is None:
            properties = dict()

        properties['run_id'] = self.run_id
        posthog.capture(distinct_id=self.anonymous_user_id, event=name,
                        properties={'env': self.env_props, **properties},
                        groups={'warehouse': self.anonymous_warehouse_id})

    def track_cli_start(self, module_name: str, cli_properties: dict, command: str = None):
        try:
            props = {'cli_properties': cli_properties, 'module_name': module_name, 'command': command}
            self.send_event('cli-start', properties=props)
        except Exception:
            pass

    def track_cli_end(self, module_name: str, execution_properties: dict, command: str = None):
        try:
            props = {'execution_properties': execution_properties, 'module_name': module_name, 'command': command}
            self.send_event('cli-end', properties=props)
        except Exception:
            pass

    def track_cli_exception(self, module_name: str, exc: Exception, command: str = None) -> None:
        try:
            props = {
                'exception_properties': {
                    'exception_type': str(type(exc)),
                    'exception_content': str(exc)
                },
                'module_name': module_name,
                'command': command,
                'version': get_package_version()
            }
            self.send_event('cli-exception', properties=props)
        except Exception:
            pass

    def track_cli_help(self):
        try:
            self.send_event('cli-help')
        except Exception:
            pass

    def _get_anonymous_warehouse_id(self):
        try:
            dbt_runner = DbtRunner(monitor.paths.DBT_PROJECT_PATH, self.config.profiles_dir, self.config.profile_target)
            adapter_unique_id = dbt_runner.run_operation('get_adapter_unique_id', should_log=False)[0]
            anonymous_warehouse_id = hashlib.sha256(adapter_unique_id.encode('utf-8')).hexdigest()
            return anonymous_warehouse_id
        except Exception:
            return None
