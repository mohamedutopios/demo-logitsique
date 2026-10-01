-- Prestations annexes saisies à la main dans le portail DONUT.
select (payload->>'id')::bigint as id_saisie,
       payload->>'code_client' as code_client, payload->>'type_saisie' as prestation,
       payload->>'reference' as reference, (payload->>'valeur')::numeric as quantite,
       (payload->>'saisi_le')::timestamp as saisi_le
from ({{ derniere_version('donut', 'saisies') }}) v
