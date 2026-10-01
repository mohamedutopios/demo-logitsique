-- Temps de travail exportés de Kelio (heures au format français « 7,50 »).
select payload->>'matricule' as matricule,
       payload->>'site' as site,
       (payload->>'date')::date as jour,
       payload->>'activite' as activite,
       replace(payload->>'heures', ',', '.')::numeric as heures,
       nom_fichier
from {{ source('raw', 'fichiers') }}
where type_fichier = 'kelio'
