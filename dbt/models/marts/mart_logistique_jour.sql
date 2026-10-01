-- Reporting logistique : volumes quotidiens par site et par client.
with couples as (
  select distinct site, code_client from {{ ref('fct_commandes') }}
  union select distinct site, code_client from {{ ref('fct_receptions') }}
),
rec as (
  select site, code_client, date_reception::date as jour, count(*) as nb_receptions, sum(nb_palettes) as nb_palettes_recues,
         count(*) filter (where en_retard) as nb_receptions_en_retard
  from {{ ref('fct_receptions') }} where statut = 'CLOTUREE' group by 1, 2, 3
),
cre as (
  select site, code_client, date_creation::date as jour, count(*) as nb_commandes_creees,
         count(*) filter (where statut = 'ANNULEE') as nb_commandes_annulees
  from {{ ref('fct_commandes') }} group by 1, 2, 3
),
exp_c as (
  select site, code_client, date_expedition::date as jour, count(*) as nb_commandes_expediees,
         sum(nb_lignes) as nb_lignes_expediees, count(*) filter (where expediee_j0) as nb_expediees_j0
  from {{ ref('fct_commandes') }} where statut = 'EXPEDIEE' group by 1, 2, 3
),
exp_t as (
  select site, code_client, date_depart::date as jour, count(*) as nb_expeditions, sum(nb_colis) as nb_colis,
         sum(poids_kg) as poids_kg, sum(cout_ht) as cout_transport_ht
  from {{ ref('fct_expeditions') }} group by 1, 2, 3
)
select d.jour, c.site, c.code_client,
       coalesce(rec.nb_receptions, 0) as nb_receptions,
       coalesce(rec.nb_palettes_recues, 0) as nb_palettes_recues,
       coalesce(rec.nb_receptions_en_retard, 0) as nb_receptions_en_retard,
       coalesce(cre.nb_commandes_creees, 0) as nb_commandes_creees,
       coalesce(cre.nb_commandes_annulees, 0) as nb_commandes_annulees,
       coalesce(exp_c.nb_commandes_expediees, 0) as nb_commandes_expediees,
       coalesce(exp_c.nb_lignes_expediees, 0) as nb_lignes_expediees,
       round(100.0 * exp_c.nb_expediees_j0 / nullif(exp_c.nb_commandes_expediees, 0), 1) as taux_expedition_j0_pct,
       coalesce(exp_t.nb_expeditions, 0) as nb_expeditions,
       coalesce(exp_t.nb_colis, 0) as nb_colis,
       coalesce(exp_t.poids_kg, 0) as poids_kg,
       coalesce(exp_t.cout_transport_ht, 0) as cout_transport_ht
from {{ ref('dim_date') }} d
cross join couples c
left join rec   on rec.jour = d.jour and rec.site = c.site and rec.code_client = c.code_client
left join cre   on cre.jour = d.jour and cre.site = c.site and cre.code_client = c.code_client
left join exp_c on exp_c.jour = d.jour and exp_c.site = c.site and exp_c.code_client = c.code_client
left join exp_t on exp_t.jour = d.jour and exp_t.site = c.site and exp_t.code_client = c.code_client
where d.jour <= current_date
