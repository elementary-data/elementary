import os
from contextlib import contextmanager
from typing import Dict, Generator, Optional


@contextmanager
def env_vars_context(env_vars: Optional[Dict[str, str]]) -> Generator[None, None, None]:
    if env_vars is None:
        yield
        return

    original_env_vars = os.environ.copy()
    os.environ.update(env_vars)
    yield

    for key in env_vars:
        if key not in original_env_vars:
            del os.environ[key]
        elif original_env_vars[key] != env_vars[key]:
            os.environ[key] = original_env_vars[key]
