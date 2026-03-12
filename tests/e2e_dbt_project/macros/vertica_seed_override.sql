{#- Override the dbt-vertica seed helper so that each seed file uses a
    unique reject-table name.  The upstream macro hardcodes
    ``seed_rejects`` for every seed, which causes "Object already exists"
    errors when ``dbt seed`` processes more than one file. -#}
{% macro copy_local_load_csv_rows(model, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}

    {#- Build a per-seed reject table name so concurrent seeds don't clash. -#}
    {% set reject_table = model["alias"] ~ "_rejects" %}

    {% set sql %}
        copy {{ this.render() }}
        ({{ cols_sql }})
        from local '{{ agate_table.original_abspath }}'
        delimiter ','
        enclosed by '"'
        skip 1
        abort on error
        rejected data as table {{ this.without_identifier() }}.{{ reject_table }};
    {% endset %}

    {{ return(sql) }}
{% endmacro %}
