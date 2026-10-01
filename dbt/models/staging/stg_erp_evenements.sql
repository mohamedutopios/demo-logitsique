-- Événements publiés sur Kafka par l'ERP du client CLI004.
select payload->>'client' as code_client,
       payload->>'num_commande' as num_commande,
       payload->>'evenement' as evenement,
       (payload->>'horodatage')::timestamp as horodatage,
       (payload->>'nb_lignes')::int as nb_lignes,
       topic, partition, position, recu_le
from {{ source('raw', 'evenements') }}
where topic = 'erp.commandes'
