-- Contrôle de gestion : unités traitées par heure travaillée, comparées à l'objectif.
with volumes as (
  select site, date_reception::date as jour, 'RECEPTION' as activite, sum(nb_palettes) as volume
  from {{ ref('fct_receptions') }} where statut = 'CLOTUREE' group by 1, 2
  union all
  select site, date_creation::date, 'PREPARATION', sum(nb_lignes)
  from {{ ref('fct_commandes') }} where statut <> 'ANNULEE' group by 1, 2
  union all
  select site, date_depart::date, 'EXPEDITION', sum(nb_colis)
  from {{ ref('fct_expeditions') }} group by 1, 2
),
heures as (
  select site, jour, activite, sum(heures) as heures
  from {{ ref('fct_temps_travail') }} group by 1, 2, 3
)
select h.jour, h.site, h.activite,
       coalesce(v.volume, 0) as volume,
       h.heures,
       round(coalesce(v.volume, 0) / nullif(h.heures, 0), 2) as unites_par_heure,
       o.unites_par_heure as objectif,
       round(100.0 * (coalesce(v.volume, 0) / nullif(h.heures, 0) - o.unites_par_heure) / o.unites_par_heure, 1) as ecart_objectif_pct
from heures h
left join volumes v using (site, jour, activite)
left join {{ source('ref', 'objectifs_productivite') }} o on o.code_site = h.site and o.activite = h.activite
where h.activite <> 'ADMIN'
