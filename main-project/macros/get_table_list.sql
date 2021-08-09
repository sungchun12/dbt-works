{# dbt run-operation get_table_list --args '{"project_id": "dbt-demos-sung","dataset": "dbt_bq_example","target_project_id": "dbt-demos-sung","target_dataset": "dbt_macro_example"}'  #}
{% macro get_table_list(project_id, dataset, target_project_id, target_dataset) %}
    {# this #}
    {% set get_table_query %}
        SELECT table_id as table_id FROM {{project_id}}.{{dataset}}.__TABLES__;
    {% endset %}

    {% set results = run_query(get_table_query) %}

    {% if execute %}
        {% set table_list = results.columns[0].values() %}
            {% for tbl in table_list %}
                {% set get_columns_query %}
                    SELECT 
                        column_name, data_type
                    FROM
                        {{project_id}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS
                    WHERE
                        table_name='{{tbl}}'
                        AND (data_type = 'BOOLEAN' OR 
                             data_type = 'FLOAT' OR
                             data_type = 'INTEGER' OR
                             data_type = 'NUMERIC'  )
                {% endset %}
                {% set column_results = run_query(get_columns_query)%}
                {% if execute %}
                    {% set column_list = column_results.columns[0].values() %}
                        {% set proposed_column_query %}
                            {% for col in column_list%}
                                {{find_proposed_column_for_numbers(col, project_id, dataset, tbl )}}
                                {{' union all '}} {{loop.counter}}
                            {% endfor %}
                                SELECT 
                                    column_name, 
                                    data_type AS proposed_data_type
                                FROM
                                    {{project_id}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS
                                WHERE
                                    table_name='{{tbl}}'
                                    AND (data_type not in ('BOOLEAN','FLOAT','INTEGER','NUMERIC'))
                        {% endset %}
                        {%set proposed_columns_results = run_query(proposed_column_query)%}
                            {% set create_table_query%}
                                create table {{target_project_id}}.{{target_dataset}}.{{tbl}} (
                                    {% for val in proposed_columns_results %}
                                        {{ val[0]}} {{val[1]}} ,
                                    {% endfor %}
                                )
                            {% endset %}
                            {{ log("Running some_macro: " ~ create_table_query ~ ", " ) }}
                            {% set execute_object = run_query(create_table_query) %}
                {% else %}
                    {% set column_list = [] %}
                {% endif %}
            {% endfor %}
    {% else %}
        {% set table_list = [] %}
    {% endif %}
{% endmacro %}

{% macro find_proposed_column_for_numbers(column_name, project_id, dataset_id, table_id)%}
    SELECT
        '{{column_name}}' as `Column_Name`,
        (CASE
         WHEN {{column_name}}_supports_boolean_fg = 1 
              AND {{column_name}}_min_value IN (0, 1)
              AND {{column_name}}_max_value IN (0, 1) THEN 'BOOLEAN'
         WHEN {{column_name}}_supports_int64_fg = {{column_name}}_not_missing_cnt 
         THEN 'INT64'
         WHEN {{column_name}}_supports_float64_fg = {{column_name}}_not_missing_cnt 
         THEN 'FLOAT64'
         WHEN {{column_name}}_supports_numeric_fg = {{column_name}}_not_missing_cnt 
         THEN 'NUMERIC'
         END
        ) AS proposed_data_type
    FROM (
        SELECT
            '{{column_name}}' AS column_name_to_be_updated,
            sum(CASE
                WHEN {{column_name}} IS NOT NULL THEN 1
                ELSE 0 END 
                ) AS {{column_name}}_not_missing_cnt,
            count(DISTINCT {{column_name}}) AS {{column_name}}_unique_values_cnt,
            min({{column_name}}) AS {{column_name}}_min_value,
            max({{column_name}}) AS {{column_name}}_max_value,
            (CASE
                WHEN count(distinct {{column_name}}) = 2 THEN 1
            ELSE 0 END
            ) AS {{column_name}}_supports_boolean_fg,
            sum(CASE
                WHEN safe_cast({{column_name}} AS NUMERIC) = {{column_name}} THEN 1
                ELSE 0 END
            ) AS {{column_name}}_supports_numeric_fg,
            sum(CASE
                WHEN safe_cast({{column_name}} AS FLOAT64) = {{column_name}} THEN 1
                ELSE 0 END
            ) AS {{column_name}}_supports_float64_fg,
            sum(CASE
                WHEN safe_cast({{column_name}} AS INT64) = {{column_name}} THEN 1
                ELSE 0 END
            ) AS {{column_name}}_supports_int64_fg
        FROM `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        ) 
{% endmacro %}