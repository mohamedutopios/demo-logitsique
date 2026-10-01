-- Exposé par PostgREST : chaque client ne voit que ses lignes (identité lue dans le jeton JWT).
{{ config(post_hook="ALTER VIEW {{ this }} SET (security_barrier = true)") }}
select code_client, site, num_commande, date_creation, statut_commande, date_expedition,
       nb_lignes, nb_colis, num_expedition, transporteur, statut_transport, livre_le, incident
from {{ ref('mart_client_expeditions') }}
where code_client = current_setting('request.jwt.claims', true)::json->>'client'
