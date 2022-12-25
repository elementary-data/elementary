from pydantic import BaseModel


class AlertSuppressionSchema(BaseModel):
    suppression_status: str = None
    sent_at: str = None
