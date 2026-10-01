{{ config(severity = 'warn') }}
-- Avertissement : un salarié ne peut pas dépasser 12 h de travail dans la journée.
select matricule, jour, sum(heures) as heures
from {{ ref('fct_temps_travail') }}
group by matricule, jour
having sum(heures) > 12
