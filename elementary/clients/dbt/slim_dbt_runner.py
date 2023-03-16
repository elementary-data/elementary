# flake8: noqa
import json
from typing import Optional

import dbt.adapters.factory

# IMPORTANT: This must be kept before the rest of the dbt imports
dbt.adapters.factory.get_adapter = lambda config: config.adapter

from dbt.adapters.factory import get_adapter_class_by_name, register_adapter
from dbt.config import RuntimeConfig
from dbt.flags import set_from_args
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import disable_tracking
from pydantic import BaseModel, validator

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.utils.log import get_logger

logger = get_logger(__name__)


# Disable dbt tracking
disable_tracking()

DEFAULT_VARS = "{}"


class ConfigArgs(BaseModel):
    profiles_dir: Optional[str] = None
    project_dir: str
    target: Optional[str] = None
    threads: Optional[int] = 1
    vars: Optional[str] = DEFAULT_VARS

    @validator("vars", pre=True)
    def validate_vars(cls, vars):
        if not vars:
            return DEFAULT_VARS
        return vars


class SlimDbtRunner(BaseDbtRunner):
    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        vars: dict = None,
        **kwargs,
    ):
        super().__init__(project_dir, profiles_dir, target)
        self._load_runner(
            project_dir=project_dir, profiles_dir=profiles_dir, target=target, vars=vars
        )

    def _load_runner(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        vars: dict = None,
    ):
        self._load_config_args(
            project_dir=project_dir, profiles_dir=profiles_dir, target=target, vars=vars
        )
        self._load_config()
        self._load_adapter()
        self._load_manifest()

    def _load_config_args(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        vars: dict = None,
    ):
        args = ConfigArgs(
            project_dir=project_dir,
            target=target,
            profiles_dir=profiles_dir,
            vars=json.dumps(vars) if vars else None,
        )
        set_from_args(args, args)
        self.args = args

    def _load_config(self):
        self.config = RuntimeConfig.from_args(self.args)

    def _load_adapter(self):
        register_adapter(self.config)
        self.adapter_name = self.config.credentials.type
        self.adapter = get_adapter_class_by_name(self.adapter_name)(self.config)
        self.adapter.connections.set_connection_name()
        self.config.adapter = self.adapter

    def _load_manifest(self):
        self.project_parser = ManifestLoader(
            self.config,
            self.config.load_dependencies(),
            self.adapter.connections.set_query_header,
        )
        self.manifest = self.project_parser.load()
        self.manifest.build_flat_graph()
        self.project_parser.save_macros_to_adapter(self.adapter)

    def _execute_macro(self, macro_name, **kwargs):
        if "." in macro_name:
            package_name, actual_macro_name = macro_name.split(".", 1)
        else:
            package_name = None
            actual_macro_name = macro_name

        return self.adapter.execute_macro(
            macro_name=actual_macro_name,
            project=package_name,
            kwargs=kwargs,
            manifest=self.manifest,
        )

    def close_connection(self):
        self.adapter.connections.cleanup_all()

    def run_operation(
        self,
        macro_name: str,
        capture_output: bool = True,
        macro_args: dict = dict(),
        log_errors: bool = True,
        vars: Optional[dict] = None,
        quiet: bool = False,
        **kwargs,
    ) -> list:
        if vars:
            # vars are being parsed as part of the manifest
            self._load_runner(
                project_dir=self.args.project_dir,
                profiles_dir=self.args.profiles_dir,
                target=self.args.target,
                vars=json.dumps(vars),
            )

        log_message = f"Running dbt run-operation {macro_name} --args {macro_args}{f' --var {vars}' if vars else ''}"
        if not quiet:
            logger.info(log_message)
        else:
            logger.debug(log_message)

        run_operation_results = []
        macro_output = self._execute_macro(macro_name, **macro_args)
        if capture_output:
            run_operation_results = [json.dumps(macro_output)]
        return run_operation_results

    def deps(self, *args, **kwargs):
        raise NotImplementedError

    def seed(self, *args, **kwargs):
        raise NotImplementedError

    def snapshot(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def test(self, *args, **kwargs):
        raise NotImplementedError

    def debug(self, *args, **kwargs):
        raise NotImplementedError

    def ls(self, *args, **kwargs):
        raise NotImplementedError

    def source_freshness(self, *args, **kwargs):
        raise NotImplementedError
