-- Facturation : volumes quotidiens valorisés au tarif en vigueur ce jour-là, cumulés par mois.
with volumes as (
  select code_client, date_reception::date as jour, 'RECEPTION_PALETTE' as prestation, nb_palettes::numeric as quantite
  from {{ ref('fct_receptions') }} where statut = 'CLOTUREE'
  union all
  select code_client, date_expedition::date, 'PREPARATION_LIGNE', nb_lignes
  from {{ ref('fct_commandes') }} where statut = 'EXPEDIEE'
  union all
  select code_client, date_depart::date, 'EXPEDITION_COLIS', nb_colis
  from {{ ref('fct_expeditions') }}
  union all
  select code_client, jour, prestation, quantite
  from {{ ref('fct_prestations_annexes') }}
),
valorise as (
  select v.code_client, v.jour, v.prestation, v.quantite, t.prix_unitaire,
         v.quantite * t.prix_unitaire as montant_ht
  from volumes v
  left join {{ source('ref', 'tarifs') }} t
    on t.code_client = v.code_client and t.prestation = v.prestation
   and v.jour >= t.valide_du and (t.valide_au is null or v.jour <= t.valide_au)
)
select date_trunc('month', jour)::date as mois, code_client, prestation,
       sum(quantite) as quantite,
       round(sum(montant_ht) / nullif(sum(quantite), 0), 4) as prix_moyen,
       round(sum(montant_ht), 2) as montant_ht,
       count(*) filter (where prix_unitaire is null) as jours_sans_tarif
from valorise
group by 1, 2, 3
