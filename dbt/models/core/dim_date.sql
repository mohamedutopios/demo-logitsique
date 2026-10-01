-- Calendrier : du premier jour d'activité à J+14 (pour les prévisions).
with bornes as (
  select least(min(date_creation)::date, current_date) as debut from {{ ref('stg_commandes') }}
)
select d::date as jour,
       extract(isodow from d)::int as jour_semaine,          -- 1 = lundi … 7 = dimanche
       to_char(d, 'TMDay') as nom_jour,
       date_trunc('month', d)::date as mois,
       extract(week from d)::int as semaine_iso,
       extract(isodow from d) < 6 as jour_ouvre
from bornes, generate_series(bornes.debut, current_date + 14, interval '1 day') as d
