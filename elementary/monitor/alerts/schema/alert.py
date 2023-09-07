from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel


@dataclass
class ReportLinkData:
    url: str
    text: str


class AlertSuppressionSchema(BaseModel):
    suppression_status: Optional[str] = None
    sent_at: Optional[str] = None
