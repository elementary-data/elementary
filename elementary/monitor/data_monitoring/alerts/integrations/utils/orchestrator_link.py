from typing import Optional

from elementary.messages.blocks import Icon
from elementary.monitor.alerts.alert import OrchestratorInfo
from elementary.utils.pydantic_shim import BaseModel


class OrchestratorLinkData(BaseModel):
    url: str
    text: str
    orchestrator: str
    icon: Optional[Icon] = None


def create_orchestrator_link(
    orchestrator_info: OrchestratorInfo,
) -> Optional[OrchestratorLinkData]:
    """Create an orchestrator link from orchestrator info if URL is available."""
    if not orchestrator_info or not orchestrator_info.run_url:
        return None

    orchestrator = orchestrator_info.orchestrator or "orchestrator"

    return OrchestratorLinkData(
        url=orchestrator_info.run_url,
        text=f"View in {orchestrator}",
        orchestrator=orchestrator,
        icon=Icon.LINK,
    )


def create_job_link(
    orchestrator_info: OrchestratorInfo,
) -> Optional[OrchestratorLinkData]:
    """Create a job-level orchestrator link if job URL is available."""
    if not orchestrator_info or not orchestrator_info.job_url:
        return None

    orchestrator = orchestrator_info.orchestrator or "orchestrator"
    job_name = orchestrator_info.job_name or "Job"

    # Capitalize orchestrator name for display
    display_name = orchestrator.replace("_", " ").title()

    return OrchestratorLinkData(
        url=orchestrator_info.job_url,
        text=f"{job_name} in {display_name}",
        orchestrator=orchestrator,
        icon=Icon.GEAR,
    )
