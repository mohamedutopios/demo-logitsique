-- Expéditions du TMS ; le code client est celui du WMS du site d'origine.
select payload->>'num_expedition' as num_expedition,
       (payload->>'site') || '-' || (payload->>'num_commande') as id_commande,
       payload->>'num_commande' as num_commande,
       payload->>'site' as site,
       cc.code_client,
       payload->>'code_client' as code_source,
       payload->>'transporteur' as transporteur,
       (payload->>'date_depart')::timestamp as date_depart,
       (payload->>'poids_kg')::numeric as poids_kg,
       (payload->>'nb_colis')::int as nb_colis,
       (payload->>'cout_ht')::numeric as cout_ht,
       v.maj_source
from ({{ derniere_version('tms', 'expeditions') }}) v
join {{ source('ref', 'sites') }} s on s.code_site = v.payload->>'site'
left join {{ source('ref', 'correspondance_clients') }} cc on cc.source = s.wms and cc.code_source = v.payload->>'code_client'
