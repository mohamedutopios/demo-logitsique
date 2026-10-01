select id_reception, site, num_reception, code_client, date_prevue, date_reception, statut, nb_palettes,
       date_reception is not null and date_reception::date > date_prevue as en_retard,
       maj_source
from {{ ref('stg_receptions') }}
