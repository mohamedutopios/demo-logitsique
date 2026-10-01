select id_commande, site, num_commande, code_client, date_creation, date_expedition, statut,
       nb_lignes, nb_colis,
       round(extract(epoch from (date_expedition - date_creation)) / 3600.0, 2) as delai_expedition_h,
       date_expedition::date = date_creation::date as expediee_j0,
       maj_source
from {{ ref('stg_commandes') }}
