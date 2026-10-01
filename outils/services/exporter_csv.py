"""Exports CSV destinés aux clients : un dossier par client dans /exports.

Utilisation : python services/exporter_csv.py [--jour AAAA-MM-JJ]   (par défaut : hier)
Produit, pour chaque client actif :
  /exports/<client>/expeditions_<jour>.csv   commandes créées ou expédiées ce jour-là
  /exports/<client>/stock_<jour>.csv         dernière photo de stock
"""
import argparse, csv, os
from datetime import date, timedelta
import psycopg

DWH_DSN = os.environ.get("DWH_LECTURE_DSN", "postgresql://lecture_bi:lecture_bi@dwh:5432/dwh")
EXPORTS = os.environ.get("EXPORTS", "/exports")


def ecrire(chemin, cur):
    with open(chemin, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([c.name for c in cur.description])
        n = 0
        for row in cur:
            w.writerow(["" if v is None else v for v in row])
            n += 1
    return n


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--jour", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    jour = p.parse_args().jour
    with psycopg.connect(DWH_DSN) as cx, cx.cursor() as cur:
        cur.execute("SELECT code_client FROM ref.clients WHERE actif ORDER BY 1")
        for (client,) in cur.fetchall():
            dossier = os.path.join(EXPORTS, client)
            os.makedirs(dossier, exist_ok=True)
            with cx.cursor() as c2:
                c2.execute("""SELECT site, num_commande, date_creation, statut_commande, date_expedition,
                                     nb_lignes, nb_colis, num_expedition, transporteur, statut_transport, livre_le
                              FROM marts.mart_client_expeditions
                              WHERE code_client = %s AND (date_creation::date = %s OR date_expedition::date = %s)
                              ORDER BY date_creation""", (client, jour, jour))
                n1 = ecrire(os.path.join(dossier, f"expeditions_{jour:%Y%m%d}.csv"), c2)
                c2.execute("SELECT site, sku, quantite, date_photo FROM marts.mart_client_stock WHERE code_client = %s ORDER BY site, sku", (client,))
                n2 = ecrire(os.path.join(dossier, f"stock_{jour:%Y%m%d}.csv"), c2)
            print(f"{client} : {n1} lignes d'expéditions, {n2} lignes de stock → {dossier}")


if __name__ == "__main__":
    main()
