{# Tests génériques maison, sans dépendance externe. #}
{% test valeur_positive(model, column_name) %}
  select * from {{ model }} where {{ column_name }} < 0
{% endtest %}

{% test combinaison_unique(model, colonnes) %}
  select {{ colonnes | join(', ') }}, count(*) as n
  from {{ model }}
  group by {{ colonnes | join(', ') }}
  having count(*) > 1
{% endtest %}
