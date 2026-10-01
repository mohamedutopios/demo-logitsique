-- Une ligne par salarié, jour et activité.
select matricule, site, jour, activite, sum(heures) as heures
from {{ ref('stg_temps_kelio') }}
group by matricule, site, jour, activite
