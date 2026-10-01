-- Commandes des deux WMS, harmonisées.
with nord as (
  select 'LIL' as site, 'wms_nord' as source_client, 'wms_nord' as source_statut,
         payload->>'num_commande' as num_commande, payload->>'code_client' as code_source,
         (payload->>'date_creation')::timestamp as date_creation, (payload->>'date_expedition')::timestamp as date_expedition,
         payload->>'statut' as statut_source, (payload->>'nb_lignes')::int as nb_lignes, (payload->>'nb_colis')::int as nb_colis, maj_source
  from ({{ derniere_version('wms_nord', 'commandes') }}) v
), sud as (
  select 'LYO', 'wms_sud', 'wms_sud_outbound',
         payload->>'ref', payload->>'customer',
         (payload->>'created_at')::timestamp, (payload->>'shipped_at')::timestamp,
         payload->>'status', (payload->>'lines')::int, (payload->>'parcels')::int, maj_source
  from ({{ derniere_version('wms_sud', 'outbound') }}) v
), unies as (select * from nord union all select * from sud)
select u.site || '-' || u.num_commande as id_commande, u.site, u.num_commande,
       cc.code_client, u.code_source, u.date_creation, u.date_expedition,
       st.statut, u.statut_source, u.nb_lignes, u.nb_colis, u.maj_source
from unies u
left join {{ source('ref', 'correspondance_clients') }} cc on cc.source = u.source_client and cc.code_source = u.code_source
left join {{ source('ref', 'statuts') }} st on st.source = u.source_statut and st.code_source = u.statut_source
