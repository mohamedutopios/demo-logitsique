{# Dernière version connue de chaque ligne d'une table source (la zone brute garde tout l'historique). #}
{% macro derniere_version(source_name, table_name) -%}
  select distinct on (cle) cle, maj_source, payload
  from {{ source('raw', 'extraction') }}
  where source = '{{ source_name }}' and table_source = '{{ table_name }}'
  order by cle, maj_source desc, extrait_le desc
{%- endmacro %}
