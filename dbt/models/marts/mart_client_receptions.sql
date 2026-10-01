select code_client, site, num_reception, date_prevue, date_reception, statut, nb_palettes, en_retard, maj_source as derniere_maj
from {{ ref('fct_receptions') }}
