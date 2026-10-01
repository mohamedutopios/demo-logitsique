-- Contrôles de rapprochement entre sources : ce qu'aucune source ne voit seule.
select 'EDI_SANS_COMMANDE_WMS' as controle, 'ELEVEE' as gravite, e.code_client, e.num_commande as reference,
       'Commande reçue en EDI mais absente des WMS' as detail
from (select distinct code_client, num_commande from {{ ref('stg_edi_commandes') }}) e
left join {{ ref('fct_commandes') }} c on c.num_commande = e.num_commande
where c.num_commande is null
union all
select 'COMMANDE_WMS_SANS_EVENEMENT_ERP', 'MOYENNE', c.code_client, c.num_commande,
       'Commande CLI004 présente dans le WMS sans événement COMMANDE_CREEE reçu de l''ERP'
from {{ ref('fct_commandes') }} c
left join {{ ref('stg_erp_evenements') }} ev on ev.num_commande = c.num_commande
where c.code_client = 'CLI004' and ev.num_commande is null
  and exists (select 1 from {{ ref('stg_erp_evenements') }})
union all
select 'EXPEDITION_SANS_COMMANDE', 'ELEVEE', e.code_client, e.num_expedition,
       'Expédition TMS sans commande WMS correspondante'
from {{ ref('fct_expeditions') }} e
left join {{ ref('fct_commandes') }} c using (id_commande)
where c.id_commande is null
union all
select 'CLIENT_NON_REFERENCE', 'ELEVEE', coalesce(code_client, code_source), id_commande,
       'Code client absent de ref.correspondance_clients'
from {{ ref('stg_commandes') }} where code_client is null
union all
select 'HEURES_EXCESSIVES', 'MOYENNE', null, matricule || ' ' || jour,
       'Plus de 12 h déclarées dans Kelio sur la journée'
from {{ ref('fct_temps_travail') }} group by matricule, jour having sum(heures) > 12
