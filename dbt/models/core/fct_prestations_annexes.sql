select id_saisie, code_client, prestation, reference, quantite, saisi_le, saisi_le::date as jour
from {{ ref('stg_saisies_donut') }}
