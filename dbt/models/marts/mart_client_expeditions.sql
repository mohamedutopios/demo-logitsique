-- Suivi des commandes et expéditions exposé aux clients (portail, exports, événements).
select c.code_client, c.site, c.num_commande, c.date_creation, c.statut as statut_commande,
       c.date_expedition, c.nb_lignes, c.nb_colis,
       e.num_expedition, e.transporteur, e.statut_transport, e.livre_le, e.incident,
       greatest(c.maj_source, e.derniere_maj_transport) as derniere_maj
from {{ ref('fct_commandes') }} c
left join {{ ref('fct_expeditions') }} e using (id_commande)
