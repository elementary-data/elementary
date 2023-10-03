from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseDbtRunner(ABC):
    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
        vars: Optional[dict] = None,
        secret_vars: Optional[dict] = None,
        allow_macros_without_package_prefix: bool = False,
    ) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.vars = vars or {}
        self.secret_vars = secret_vars or {}
        self.allow_macros_without_package_prefix = allow_macros_without_package_prefix

    @abstractmethod
    def deps(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def seed(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def snapshot(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def run_operation(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def test(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def debug(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def ls(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def source_freshness(self, *args, **kwargs):
        raise NotImplementedError

    def _get_all_vars(self, vars: Optional[Dict[str, Any]] = None):
        return {
            **self.vars,
            **self.secret_vars,
            **(vars or {}),
        }

    def _get_secret_masked_vars(self, vars: Dict[str, Any]):
        return {k: v if k not in self.secret_vars else "***" for k, v in vars.items()}
