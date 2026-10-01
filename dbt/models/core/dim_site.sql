select code_site, nom, wms from {{ source('ref', 'sites') }}
