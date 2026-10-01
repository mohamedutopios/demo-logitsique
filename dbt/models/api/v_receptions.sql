{{ config(post_hook="ALTER VIEW {{ this }} SET (security_barrier = true)") }}
select code_client, site, num_reception, date_prevue, date_reception, statut, nb_palettes, en_retard
from {{ ref('mart_client_receptions') }}
where code_client = current_setting('request.jwt.claims', true)::json->>'client'
