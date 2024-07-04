import json
from dataclasses import dataclass
from typing import Iterator, Optional

from elementary.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class DbtLog:
    msg: Optional[str]
    level: Optional[str]
    exception: Optional[str]

    @classmethod
    def from_log_line(cls, log_line: str) -> "DbtLog":
        log = json.loads(log_line)
        return cls(
            msg=log.get("info", {}).get("msg") or log.get("data", {}).get("msg"),
            level=log.get("info", {}).get("level") or log.get("level"),
            exception=log.get("info", {}).get("exc") or log.get("data", {}).get("exc"),
        )

    def __str__(self) -> str:
        as_string = f"{self.level or 'unknown'}: {self.msg}"
        if self.exception:
            as_string += f"\nError:\n{self.exception}"
        return as_string


def parse_dbt_output(output: str, log_format: str = "json") -> Iterator[DbtLog]:
    for log_line in output.strip().splitlines():
        try:
            if log_format == "json":
                yield DbtLog.from_log_line(log_line)
            elif log_format == "text":
                yield DbtLog(msg=log_line, level="info", exception=None)
        except json.JSONDecodeError:
            logger.debug(f"Unable to parse dbt log message: {log_line}", exc_info=True)
