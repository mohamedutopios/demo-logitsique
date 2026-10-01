{# Utilise le nom de schéma demandé tel quel (staging, core, marts, api) au lieu de « staging_core ». #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
