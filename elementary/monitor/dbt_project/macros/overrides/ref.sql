{#
  Override adapter-specific ref() macros (e.g. dbt-dremio) that don't properly
  handle the two-argument positional form: ref('package', 'model').

  dbt-dremio's ref(model_name, v=None) intercepts the second positional arg as
  a version identifier, breaking cross-package refs like ref('elementary', 'dbt_models').

  This override forwards all arguments directly to builtins.ref(), which correctly
  handles all ref() forms in dbt-core 1.11+:
    - ref('model')                      -> single model lookup
    - ref('package', 'model')           -> cross-package ref
    - ref('model', v=1)                 -> versioned ref
    - ref('model', version=1)           -> versioned ref (alias)
    - ref('model', package='package')   -> cross-package ref (legacy keyword form)
#}

{%- macro ref(model_name_or_package, model_name=none, v=none, version=none, package=none) -%}
  {%- set effective_version = v if v is not none else version -%}
  {%- if model_name is not none -%}
    {#- Two-arg positional: ref('package', 'model') -#}
    {%- if effective_version is not none -%}
      {%- do return(builtins.ref(model_name_or_package, model_name, v=effective_version)) -%}
    {%- else -%}
      {%- do return(builtins.ref(model_name_or_package, model_name)) -%}
    {%- endif -%}
  {%- elif package is not none -%}
    {#- Legacy keyword: ref('model', package='pkg') -#}
    {%- if effective_version is not none -%}
      {%- do return(builtins.ref(package, model_name_or_package, v=effective_version)) -%}
    {%- else -%}
      {%- do return(builtins.ref(package, model_name_or_package)) -%}
    {%- endif -%}
  {%- elif effective_version is not none -%}
    {#- Versioned: ref('model', v=1) or ref('model', version=1) -#}
    {%- do return(builtins.ref(model_name_or_package, v=effective_version)) -%}
  {%- else -%}
    {#- Simple: ref('model') -#}
    {%- do return(builtins.ref(model_name_or_package)) -%}
  {%- endif -%}
{%- endmacro -%}
