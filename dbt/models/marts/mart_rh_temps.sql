-- Reporting RH : heures et effectifs par site, jour et activité (source Kelio).
select jour, site, activite,
       sum(heures) as heures,
       count(distinct matricule) as effectif
from {{ ref('fct_temps_travail') }}
group by jour, site, activite
