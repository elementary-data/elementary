from typing import Optional

from pydantic import BaseModel


class AlertSuppressionSchema(BaseModel):
    suppression_status: Optional[str] = None
    sent_at: Optional[str] = None
