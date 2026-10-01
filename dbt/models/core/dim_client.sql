select code_client, nom, actif from {{ source('ref', 'clients') }}
