"""Base class for external seed loaders."""

from __future__ import annotations

import csv
import glob
import os
import shlex
import subprocess
from abc import ABC, abstractmethod


class ExternalSeeder(ABC):
    """Common interface for loading seed CSVs into a warehouse externally."""

    def __init__(self, data_dir: str, schema_name: str) -> None:
        self.data_dir = os.path.abspath(data_dir)
        self.schema_name = schema_name

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def run(cmd: list[str], check: bool = True, **kw: object) -> subprocess.CompletedProcess:  # type: ignore[type-arg]
        """Run a command, printing it first."""
        print(f"  -> {shlex.join(cmd)}")
        return subprocess.run(cmd, check=check, **kw)

    @staticmethod
    def csv_has_data(path: str) -> bool:
        """Return *True* when the CSV has a header AND at least one data row."""
        with open(path) as f:
            reader = csv.reader(f)
            try:
                next(reader)  # header
                next(reader)  # first data row
                return True
            except StopIteration:
                return False

    @staticmethod
    def csv_columns(path: str) -> list[str]:
        """Return the header row of a CSV (empty list when file is empty)."""
        with open(path) as f:
            reader = csv.reader(f)
            try:
                return next(reader)
            except StopIteration:
                return []

    def iter_seed_csvs(self):
        """Yield ``(subdir, csv_path, table_name)`` for every seed CSV."""
        for subdir in ("training", "validation"):
            csv_dir = os.path.join(self.data_dir, subdir)
            for csv_path in sorted(glob.glob(os.path.join(csv_dir, "*.csv"))):
                fname = os.path.basename(csv_path)
                table_name = fname.replace(".csv", "")
                yield subdir, csv_path, table_name

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @abstractmethod
    def load(self) -> None:
        """Load all seed CSVs into the target warehouse."""
