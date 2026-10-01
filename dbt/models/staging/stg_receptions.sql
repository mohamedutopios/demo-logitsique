-- Réceptions des deux WMS, harmonisées : même site, même code client, mêmes statuts.
with nord as (
  select 'LIL' as site, 'wms_nord' as source_client, 'wms_nord' as source_statut,
         payload->>'num_reception' as num_reception, payload->>'code_client' as code_source,
         (payload->>'date_prevue')::date as date_prevue, (payload->>'date_reception')::timestamp as date_reception,
         payload->>'statut' as statut_source, (payload->>'nb_palettes')::int as nb_palettes, maj_source
  from ({{ derniere_version('wms_nord', 'receptions') }}) v
), sud as (
  select 'LYO', 'wms_sud', 'wms_sud_inbound',
         payload->>'ref', payload->>'customer',
         (payload->>'expected_at')::date, (payload->>'received_at')::timestamp,
         payload->>'status', (payload->>'pallets')::int, maj_source
  from ({{ derniere_version('wms_sud', 'inbound') }}) v
), unies as (select * from nord union all select * from sud)
select u.site || '-' || u.num_reception as id_reception, u.site, u.num_reception,
       cc.code_client, u.code_source, u.date_prevue, u.date_reception,
       st.statut, u.statut_source, u.nb_palettes, u.maj_source
from unies u
left join {{ source('ref', 'correspondance_clients') }} cc on cc.source = u.source_client and cc.code_source = u.code_source
left join {{ source('ref', 'statuts') }} st on st.source = u.source_statut and st.code_source = u.statut_source
