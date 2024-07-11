import os
from contextlib import contextmanager


@contextmanager
def with_chdir(path: str):
    curdir = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(curdir)
