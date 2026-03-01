import os
import ssl
from typing import Optional

import certifi

from elementary.utils.log import get_logger

logger = get_logger(__name__)

CERTIFI = "certifi"
SYSTEM = "system"


def create_ssl_context(ssl_ca_bundle: Optional[str] = None) -> Optional[ssl.SSLContext]:
    """Resolve an ssl_ca_bundle setting into an SSLContext.

    Returns ``None`` when *ssl_ca_bundle* is ``None`` so that each
    library keeps its own default CA behaviour.
    """
    if ssl_ca_bundle is None:
        return None

    value = ssl_ca_bundle.strip()

    if value.lower() == CERTIFI:
        logger.debug("Using certifi CA bundle for SSL context.")
        return ssl.create_default_context(cafile=certifi.where())

    if value.lower() == SYSTEM:
        logger.debug("Using system CA store for SSL context.")
        return ssl.create_default_context()

    if not os.path.isfile(value):
        raise ValueError(f"ssl_ca_bundle path does not exist or is not a file: {value}")
    logger.debug("Using custom CA bundle for SSL context: %s", value)
    return ssl.create_default_context(cafile=value)
