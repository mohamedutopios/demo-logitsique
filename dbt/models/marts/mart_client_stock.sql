-- Dernière photo de stock connue par client et par site.
select s.code_client, s.site, s.sku, s.quantite, s.date_photo
from {{ ref('stg_stocks') }} s
join (select site, max(date_photo) as date_photo from {{ ref('stg_stocks') }} group by site) d using (site, date_photo)
