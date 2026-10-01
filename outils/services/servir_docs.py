"""Sert la documentation dbt (lignage, descriptions, tests) sur le port 8083.

La documentation est un site statique produit par « dbt docs generate » dans dbt/target/.
Tant qu'elle n'a pas été générée, une page explique la commande à lancer ; dès qu'elle existe,
elle est servie, sans redémarrer le conteneur.

Démarré par docker compose (service dbt-docs) : python services/servir_docs.py
"""
import functools
import http.server
import os

CIBLE = os.environ.get("DBT_TARGET", "/app/dbt/target")
ATTENTE = """<!doctype html><html lang="fr"><meta charset="utf-8"><title>Documentation dbt</title>
<body style="font-family:system-ui;max-width:640px;margin:60px auto;line-height:1.6">
<h1>Documentation dbt pas encore générée</h1>
<p>Lancez, depuis le dossier du kit :</p>
<pre style="background:#f3f3f3;padding:12px">docker compose run --rm outils dbt docs generate --project-dir dbt --profiles-dir dbt</pre>
<p>puis rechargez cette page.</p></body></html>""".encode()


class Gestionnaire(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if not os.path.exists(os.path.join(CIBLE, "index.html")):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(ATTENTE)
            return
        super().do_GET()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print("Documentation dbt servie sur http://localhost:8083", flush=True)
    http.server.ThreadingHTTPServer(("0.0.0.0", 8083), functools.partial(Gestionnaire, directory=CIBLE)).serve_forever()
