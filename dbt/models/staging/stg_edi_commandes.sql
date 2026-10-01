-- Lignes des fichiers EDI ORDERS reçus du client CLI003.
select split_part(nom_fichier, '_', 2) as code_client,
       payload->>'num_commande' as num_commande,
       payload->>'sku' as sku,
       (payload->>'quantite')::int as quantite,
       (payload->>'date_souhaitee')::date as date_souhaitee,
       nom_fichier
from {{ source('raw', 'fichiers') }}
where type_fichier = 'edi_orders'
