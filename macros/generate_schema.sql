{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {%- if 'bronze' in node.path -%}
            bronze
        {%- elif 'silver' in node.path -%}
            silver
        {%- elif 'gold' in node.path -%}
            gold
        {%- else -%}
            {{ target.schema }}
        {%- endif -%}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}