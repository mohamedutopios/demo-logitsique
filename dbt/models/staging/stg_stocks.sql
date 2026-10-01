-- Photos de stock quotidiennes des deux WMS.
with unies as (
  select 'LIL' as site, 'wms_nord' as source_client, payload->>'code_client' as code_source,
         payload->>'sku' as sku, (payload->>'quantite')::int as quantite, (payload->>'date_photo')::date as date_photo
  from ({{ derniere_version('wms_nord', 'stock') }}) v
  union all
  select 'LYO', 'wms_sud', payload->>'customer', payload->>'item', (payload->>'qty')::int, (payload->>'snapshot_date')::date
  from ({{ derniere_version('wms_sud', 'inventory') }}) v
)
select u.site, cc.code_client, u.code_source, u.sku, u.quantite, u.date_photo
from unies u
left join {{ source('ref', 'correspondance_clients') }} cc on cc.source = u.source_client and cc.code_source = u.code_source
