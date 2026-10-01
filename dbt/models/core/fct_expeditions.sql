-- Une ligne par expédition, avec la synthèse du suivi transporteur.
with suivi as (
  select num_expedition,
         min(horodatage) filter (where statut = 'PRIS_EN_CHARGE') as pris_en_charge_le,
         min(horodatage) filter (where statut = 'LIVRE') as livre_le,
         bool_or(statut = 'INCIDENT') as incident,
         (array_agg(statut order by horodatage desc))[1] as dernier_statut,
         max(horodatage) as derniere_maj_transport
  from {{ ref('stg_suivi_transporteurs') }}
  group by num_expedition
)
select e.num_expedition, e.id_commande, e.num_commande, e.site, e.code_client, e.transporteur,
       e.date_depart, e.poids_kg, e.nb_colis, e.cout_ht,
       coalesce(s.dernier_statut, 'EN_ATTENTE_TRANSPORTEUR') as statut_transport,
       s.pris_en_charge_le, s.livre_le, coalesce(s.incident, false) as incident, s.derniere_maj_transport,
       round(extract(epoch from (s.livre_le - e.date_depart)) / 3600.0, 1) as delai_livraison_h
from {{ ref('stg_expeditions') }} e
left join suivi s using (num_expedition)
