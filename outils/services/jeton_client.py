"""Fabrique un jeton JWT pour tester le portail client (PostgREST).

Utilisation : python services/jeton_client.py CLI003
Le jeton porte le rôle PostgreSQL « portail_client » et le code client : les vues du schéma api
ne renvoient que les lignes de ce client.
"""
import os, sys, time
import jwt

SECRET = os.environ.get("PGRST_JWT_SECRET", "cle-secrete-de-demonstration-32-caracteres-min")

if len(sys.argv) != 2:
    sys.exit("Usage : python services/jeton_client.py CLI003")
print(jwt.encode({"role": "portail_client", "client": sys.argv[1], "exp": int(time.time()) + 8 * 3600},
                 SECRET, algorithm="HS256"))
