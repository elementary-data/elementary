from typing import List, Optional

from pydantic import BaseModel

from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DataMonitoringAlertsFilter(BaseModel):
    tag: Optional[str] = None
    owner: Optional[str] = None
    model: Optional[str] = None
    node_names: Optional[List[str]] = None
