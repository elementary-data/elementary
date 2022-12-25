from pydantic import BaseModel


class AlertSuppressionSchema(BaseModel):
    suppression_status: str
    sent_at: str
