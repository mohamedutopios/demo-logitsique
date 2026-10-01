-- Événements de suivi des transporteurs : GEODIS (JSON) et DPD (CSV), formats et codes différents.
with geodis as (
  select 'GEODIS' as transporteur, 'geodis' as source_statut,
         payload->>'expedition' as num_expedition, payload->>'statut' as code_evenement,
         (payload->>'horodatage')::timestamp as horodatage, nom_fichier
  from {{ source('raw', 'fichiers') }} where type_fichier = 'transporteur_geodis'
), dpd as (
  select 'DPD', 'dpd', payload->>'ref_expedition', payload->>'code_evenement',
         to_timestamp(payload->>'date_heure', 'DD/MM/YYYY HH24:MI')::timestamp, nom_fichier
  from {{ source('raw', 'fichiers') }} where type_fichier = 'transporteur_dpd'
), unies as (select * from geodis union all select * from dpd)
select distinct u.transporteur, u.num_expedition, u.code_evenement, st.statut, u.horodatage, u.nom_fichier
from unies u
left join {{ source('ref', 'statuts') }} st on st.source = u.source_statut and st.code_source = u.code_evenement
