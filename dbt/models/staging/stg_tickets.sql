select (payload->>'id')::bigint as id_ticket,
       payload->>'code_client' as code_client, payload->>'site' as site,
       payload->>'categorie' as categorie, payload->>'priorite' as priorite,
       (payload->>'ouvert_le')::timestamp as ouvert_le, (payload->>'resolu_le')::timestamp as resolu_le,
       payload->>'statut' as statut, maj_source
from ({{ derniere_version('ticketing', 'tickets') }}) v
