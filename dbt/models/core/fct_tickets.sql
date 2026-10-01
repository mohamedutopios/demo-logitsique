select id_ticket, code_client, site, categorie, priorite, ouvert_le, resolu_le, statut,
       round(extract(epoch from (resolu_le - ouvert_le)) / 3600.0, 1) as duree_resolution_h
from {{ ref('stg_tickets') }}
