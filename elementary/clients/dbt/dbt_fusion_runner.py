import warnings

from elementary.clients.dbt.dbt2_runner import Dbt2Runner as DbtFusionRunner

warnings.warn(
    "DbtFusionRunner is deprecated, use Dbt2Runner instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["DbtFusionRunner"]
