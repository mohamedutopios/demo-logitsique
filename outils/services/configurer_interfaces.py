"""Configure Metabase et NocoDB au démarrage du kit, pour qu'ils soient prêts sans aucun clic.

- Metabase : crée le compte administrateur et ajoute l'entrepôt (rôle lecture_bi, lecture seule).
- NocoDB : le compte administrateur est créé par NocoDB lui-même (variables NC_ADMIN_*) ; ce script
  ajoute la base « Référentiels » branchée sur le schéma ref de l'entrepôt (rôle portail_ref).

Idempotent : relancé, il ne refait que ce qui manque, puis demande à Metabase de relire les tables
(utile après un dbt build). Démarré une fois à chaque « docker compose up -d » (service config-interfaces).
Relance manuelle : docker compose run --rm config-interfaces
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

METABASE = os.environ.get("METABASE_URL", "http://metabase:3000")
NOCODB = os.environ.get("NOCODB_URL", "http://nocodb:8080")
EMAIL = os.environ.get("ADMIN_EMAIL", "admin@example.com")
MOT_DE_PASSE = os.environ.get("ADMIN_MOT_DE_PASSE", "Admin-Kit-2026")
ATTENTE_MAX = 600  # secondes : Metabase met 1 à 2 minutes à démarrer


def appel(methode, url, corps=None, entetes=None):
    donnees = json.dumps(corps).encode() if corps is not None else None
    req = urllib.request.Request(url, data=donnees, method=methode,
                                 headers={"Content-Type": "application/json", **(entetes or {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            texte = r.read().decode()
            return r.status, (json.loads(texte, strict=False) if texte else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def attendre(nom, url):
    debut = time.time()
    while time.time() - debut < ATTENTE_MAX:
        try:
            statut, _ = appel("GET", url)
            if statut == 200:
                print(f"{nom} : prêt")
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(3)
    sys.exit(f"{nom} ne répond pas après {ATTENTE_MAX} s")


ENTREPOT_METABASE = {"engine": "postgres", "name": "Entrepôt logistique (dwh)",
                     "details": {"host": "dwh", "port": 5432, "dbname": "dwh",
                                 "user": "lecture_bi", "password": "lecture_bi", "ssl": False}}


def configurer_metabase():
    attendre("Metabase", f"{METABASE}/api/health")
    _, proprietes = appel("GET", f"{METABASE}/api/session/properties")
    if not proprietes.get("has-user-setup"):
        statut, reponse = appel("POST", f"{METABASE}/api/setup", {
            "token": proprietes["setup-token"],
            "user": {"email": EMAIL, "password": MOT_DE_PASSE, "first_name": "Admin", "last_name": "Kit",
                     "site_name": "Kit DWH logistique"},
            "prefs": {"site_name": "Kit DWH logistique", "site_locale": "fr", "allow_tracking": False},
        })
        if statut != 200:
            sys.exit(f"Metabase : échec de la création du compte ({statut}) {reponse}")
        print(f"Metabase : compte {EMAIL} créé")
    statut, session = appel("POST", f"{METABASE}/api/session", {"username": EMAIL, "password": MOT_DE_PASSE})
    if statut != 200:
        sys.exit(f"Metabase : connexion impossible ({statut}) {session}")
    entetes = {"X-Metabase-Session": session["id"]}
    _, bases = appel("GET", f"{METABASE}/api/database", entetes=entetes)
    entrepot = next((b for b in bases["data"] if b["engine"] == "postgres" and b["details"].get("host") == "dwh"), None)
    if entrepot is None:
        statut, entrepot = appel("POST", f"{METABASE}/api/database", ENTREPOT_METABASE, entetes)
        if statut != 200:
            sys.exit(f"Metabase : échec de l'ajout de l'entrepôt ({statut}) {entrepot}")
        print("Metabase : entrepôt ajouté (utilisateur lecture_bi)")
    # Relit la liste des tables : nécessaire après un dbt build qui a créé de nouveaux schémas ou tables
    appel("POST", f"{METABASE}/api/database/{entrepot['id']}/sync_schema", {}, entetes)
    print("Metabase : synchronisation des tables de l'entrepôt lancée")


SOURCE_REF = {"alias": "ref", "type": "pg", "is_meta": False, "inflection_column": "none", "inflection_table": "none",
              "config": {"client": "pg",
                         "connection": {"host": "dwh", "port": 5432, "user": "portail_ref",
                                        "password": "portail_ref", "database": "dwh"},
                         "searchPath": ["ref"]}}
TITRE_NOCODB = "Référentiels (entrepôt dwh)"


def base_lisible(entetes, base_id):
    """Vrai si NocoDB lit réellement les lignes de ref.tarifs (et pas seulement la liste des tables)."""
    statut, tables = appel("GET", f"{NOCODB}/api/v2/meta/bases/{base_id}/tables", entetes=entetes)
    tarifs = [t["id"] for t in (tables or {}).get("list", []) if t["title"] == "tarifs"] if statut == 200 else []
    if not tarifs:
        return False
    statut, _ = appel("GET", f"{NOCODB}/api/v2/tables/{tarifs[0]}/records?limit=1", entetes=entetes)
    return statut == 200


def configurer_nocodb():
    attendre("NocoDB", f"{NOCODB}/api/v1/health")
    statut, reponse = appel("POST", f"{NOCODB}/api/v1/auth/user/signin", {"email": EMAIL, "password": MOT_DE_PASSE})
    if statut != 200:
        sys.exit(f"NocoDB : connexion impossible ({statut}) {reponse}")
    entetes = {"xc-auth": reponse["token"]}
    # Juste après son premier démarrage, NocoDB peut créer une base qui lit le mauvais schéma (public au
    # lieu de ref) : on vérifie que les données sont lisibles, sinon on supprime et on recrée.
    for essai in range(1, 7):
        _, bases = appel("GET", f"{NOCODB}/api/v2/meta/bases", entetes=entetes)
        existante = next((b for b in bases["list"] if b["title"] == TITRE_NOCODB), None)
        if existante and base_lisible(entetes, existante["id"]):
            print(f"NocoDB : base « {TITRE_NOCODB} » opérationnelle (schéma ref, utilisateur portail_ref)")
            return
        if existante:
            appel("DELETE", f"{NOCODB}/api/v2/meta/bases/{existante['id']}", entetes=entetes)
            print(f"NocoDB : base non lisible, recréation (essai {essai})")
        time.sleep(5 * essai)
        statut, reponse = appel("POST", f"{NOCODB}/api/v2/meta/bases",
                                {"title": TITRE_NOCODB, "external": True, "sources": [SOURCE_REF]}, entetes)
        if statut != 200:
            print(f"NocoDB : création refusée ({statut}) {reponse}")
        time.sleep(3)
    sys.exit("NocoDB : impossible d'obtenir une base lisible sur le schéma ref")


if __name__ == "__main__":
    configurer_nocodb()
    configurer_metabase()
    print(f"Interfaces prêtes. Identifiants Metabase et NocoDB : {EMAIL} / {MOT_DE_PASSE}")
