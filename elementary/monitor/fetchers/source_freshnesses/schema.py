from typing import List, Optional

from elementary.utils.pydantic_shim import validator
from elementary.utils.schema import ExtendedBaseModel
from elementary.utils.time import convert_partial_iso_format_to_full_iso_format


class SourceFreshnessResultDBRowSchema(ExtendedBaseModel):
    source_freshness_execution_id: str
    unique_id: str
    max_loaded_at: Optional[str] = None
    generated_at: str
    status: str
    original_status: str
    error: Optional[str] = None
    invocation_id: str
    database_name: Optional[str] = None
    schema_name: str
    source_name: str
    table_name: str
    test_type: str
    test_sub_type: str
    loaded_at_field: Optional[str] = None
    meta: dict
    owners: Optional[List[str]]
    tags: Optional[List[str]]
    error_after: Optional[dict] = None
    warn_after: Optional[dict] = None
    filter: Optional[str] = None
    relation_name: str
    invocations_rank_index: int
    max_loaded_at_time_ago_in_s: Optional[float] = None
    freshness_description: Optional[str] = None
    snapshotted_at: Optional[str] = None

    class Config:
        smart_union = True

    @validator("generated_at", pre=True)
    def format_generated_at(cls, generated_at):
        return convert_partial_iso_format_to_full_iso_format(generated_at)

    @validator("max_loaded_at", pre=True)
    def format_max_loaded_at(cls, max_loaded_at):
        return (
            convert_partial_iso_format_to_full_iso_format(max_loaded_at)
            if max_loaded_at
            else None
        )

    @validator("snapshotted_at", pre=True)
    def format_snapshotted_at(cls, snapshotted_at):
        return (
            convert_partial_iso_format_to_full_iso_format(snapshotted_at)
            if snapshotted_at
            else None
        )

    @validator("meta", pre=True)
    def load_meta(cls, meta):
        return cls._load_var_to_dict(meta)

    @validator("tags", pre=True)
    def load_tags(cls, tags):
        return cls._load_var_to_list(tags)

    @validator("owners", pre=True)
    def load_owners(cls, owners):
        return cls._load_var_to_list(owners)

    @validator("error_after", pre=True)
    def load_error_after(cls, error_after):
        return cls._load_var_to_dict(error_after)

    @validator("warn_after", pre=True)
    def load_warn_after(cls, warn_after):
        return cls._load_var_to_dict(warn_after)
