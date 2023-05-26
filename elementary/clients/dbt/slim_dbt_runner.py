# flake8: noqa
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import dbt.adapters.factory
from packaging import version

# IMPORTANT: This must be kept before the rest of the dbt imports
dbt.adapters.factory.get_adapter = lambda config: config.adapter  # type: ignore[attr-defined]

from dbt.adapters.factory import get_adapter_class_by_name, register_adapter
from dbt.config import RuntimeConfig
from dbt.flags import set_from_args
from dbt.parser.manifest import ManifestLoader
from dbt.tracking import disable_tracking
from dbt.version import __version__ as dbt_version_string
from pydantic import BaseModel, validator

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.utils.log import get_logger

logger = get_logger(__name__)


# Disable dbt tracking
disable_tracking()

dbt_version = version.parse(dbt_version_string)

DEFAULT_VARS: Union[str, Dict[str, Any]]
if dbt_version >= version.parse("1.5.0"):
    DEFAULT_VARS = {}
else:
    DEFAULT_VARS = "{}"


def default_project_dir() -> Path:
    if "DBT_PROJECT_DIR" in os.environ:
        return Path(os.environ["DBT_PROJECT_DIR"]).resolve()
    paths = list(Path.cwd().parents)
    paths.insert(0, Path.cwd())
    return next((x for x in paths if (x / "dbt_project.yml").exists()), Path.cwd())


def default_profiles_dir() -> Path:
    if "DBT_PROFILES_DIR" in os.environ:
        return Path(os.environ["DBT_PROFILES_DIR"]).resolve()
    return (
        Path.cwd() if (Path.cwd() / "profiles.yml").exists() else Path.home() / ".dbt"
    )


DEFAULT_PROFILES_DIR = str(default_profiles_dir())
DEFAULT_PROJECT_DIR = str(default_project_dir())


class ConfigArgs(BaseModel):
    project_dir: str = DEFAULT_PROJECT_DIR
    profiles_dir: str = DEFAULT_PROFILES_DIR
    profile: Optional[str] = None
    target: Optional[str] = None
    threads: Optional[int] = 1
    vars: Optional[Union[str, Dict[str, Any]]] = DEFAULT_VARS

    @validator("vars", pre=True)
    def validate_vars(cls, vars):
        if not vars:
            return DEFAULT_VARS
        return vars


class SlimDbtRunner(BaseDbtRunner):
    def __init__(
        self,
        project_dir: str = DEFAULT_PROJECT_DIR,
        profiles_dir: str = DEFAULT_PROFILES_DIR,
        target: Optional[str] = None,
        vars: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(project_dir, profiles_dir, target)
        self._load_runner(
            project_dir=project_dir, profiles_dir=profiles_dir, target=target, vars=vars
        )

    def _load_runner(
        self,
        project_dir: str,
        profiles_dir: str,
        target: Optional[str] = None,
        vars: Optional[dict] = None,
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
        profiles_dir: str,
        target: Optional[str] = None,
        vars: Optional[dict] = None,
    ):
        config_vars: Optional[Union[str, Dict[str, Any]]] = vars
        if dbt_version < version.parse("1.5.0"):
            config_vars = json.dumps(config_vars)

        args = ConfigArgs(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=target,
            vars=config_vars,
        )
        set_from_args(args, args)  # type: ignore[arg-type]
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
                vars=vars,
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
