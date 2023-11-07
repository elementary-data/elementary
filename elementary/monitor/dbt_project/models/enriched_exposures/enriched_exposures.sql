{{
config(
  materialized = 'view',
  bind=False,
  unique_key='unique_id',
)
}}

select
    COALESCE(ee.unique_id, de.unique_id) as unique_id,
    COALESCE(ee.name, de.name) as name,
    COALESCE(ee.maturity, de.maturity) as maturity,
    COALESCE(ee.type, de.type) as type,
    COALESCE(ee.owner_email, de.owner_email) as owner_email,
    COALESCE(ee.owner_name, de.owner_name) as owner_name,
    COALESCE(ee.url, de.url) as url,
    COALESCE(ee.depends_on_macros, de.depends_on_macros) as depends_on_macros,
    COALESCE(ee.depends_on_nodes, de.depends_on_nodes) as depends_on_nodes,
    COALESCE(ee.description, de.description) as description,
    COALESCE(ee.tags, de.tags) as tags,
    COALESCE(ee.meta, de.meta) as meta,
    COALESCE(ee.package_name, de.package_name) as package_name,
    COALESCE(ee.original_path, de.original_path) as original_path,
    COALESCE(ee.path, de.path) as path,
    COALESCE(ee.generated_at, de.generated_at) as generated_at,
    COALESCE(ee.metadata_hash, de.metadata_hash) as metadata_hash,
    COALESCE(ee.label, de.label) as label,
    ee.raw_queries as raw_queries
from
    {{ ref('dbt_exposures') }} de full join {{ ref('elementary_cli', 'elementary_exposures') }} ee on ee.unique_id = de.unique_id
