{{ config(post_hook="ALTER VIEW {{ this }} SET (security_barrier = true)") }}
select code_client, site, sku, quantite, date_photo
from {{ ref('mart_client_stock') }}
where code_client = current_setting('request.jwt.claims', true)::json->>'client'
