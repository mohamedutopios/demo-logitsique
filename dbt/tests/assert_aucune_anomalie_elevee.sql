-- Bloque le build si un contrôle de gravité ÉLEVÉE est en échec.
-- Les anomalies de gravité MOYENNE restent consultables dans marts.mart_qualite_anomalies.
select * from {{ ref('mart_qualite_anomalies') }} where gravite = 'ELEVEE'
