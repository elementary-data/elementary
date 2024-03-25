"""
copied from: https://github.com/dbt-labs/dbt-semantic-interfaces/blob/main/dsi_pydantic_shim.py
"""

from importlib.metadata import version

pydantic_version = version("pydantic")
# Pydantic uses semantic versioning, i.e. <major>.<minor>.<patch>, and we need to know the major
pydantic_major = pydantic_version.split(".")[0]

if pydantic_major == "1":
    from pydantic import (  # type: ignore  # noqa
        BaseModel,
        Extra,
        Field,
        create_model,
        root_validator,
        validator,
    )
elif pydantic_major == "2":
    from pydantic.v1 import (  # type: ignore  # noqa
        BaseModel,
        Extra,
        Field,
        create_model,
        root_validator,
        validator,
    )
else:
    raise RuntimeError(
        f"Currently only pydantic 1 and 2 are supported, found pydantic {pydantic_version}"
    )
