from abc import ABC, abstractmethod
from typing import Optional


class BaseDbtRunner(ABC):
    def __init__(
        self,
        project_dir: str,
        profiles_dir: Optional[str] = None,
        target: Optional[str] = None,
    ) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target

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
