"""Publie vers Kafka les changements de statut des commandes, un topic par client
(clients.<code_client>.expeditions), pour que chaque client s'abonne à ses propres événements.

Utilisation : python services/publier_evenements.py [--afficher]
  --afficher : écrit les événements à l'écran au lieu de les publier.
Un filigrane (diffusion.filigrane) évite de republier deux fois le même changement.
"""
import argparse, json, os
from datetime import datetime
import psycopg

DWH_DSN = os.environ.get("DWH_ADMIN_DSN", "postgresql://dwh_admin:dwh_admin@dwh:5432/dwh")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--afficher", action="store_true")
    a = p.parse_args()
    producteur = None
    if not a.afficher:
        from confluent_kafka import Producer
        producteur = Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    with psycopg.connect(DWH_DSN) as cx, cx.cursor() as cur:
        cur.execute("SELECT dernier_maj FROM diffusion.filigrane WHERE canal = 'evenements_clients'")
        row = cur.fetchone()
        filigrane = row[0] if row else datetime(1900, 1, 1)
        cur.execute("""SELECT code_client, num_commande, statut_commande, date_expedition, num_expedition,
                              transporteur, statut_transport, livre_le, derniere_maj
                       FROM marts.mart_client_expeditions
                       WHERE derniere_maj > %s ORDER BY derniere_maj""", (filigrane,))
        lignes = cur.fetchall()
        par_client = {}
        for (client, num, statut, exp, num_exp, tr, st_tr, livre, maj) in lignes:
            ev = {"type": "STATUT_COMMANDE", "client": client, "num_commande": num, "statut_commande": statut,
                  "date_expedition": exp.isoformat() if exp else None, "num_expedition": num_exp,
                  "transporteur": tr, "statut_transport": st_tr, "livre_le": livre.isoformat() if livre else None,
                  "horodatage": maj.isoformat()}
            topic = f"clients.{client}.expeditions"
            par_client[topic] = par_client.get(topic, 0) + 1
            if producteur:
                producteur.produce(topic, key=num, value=json.dumps(ev, ensure_ascii=False))
                producteur.poll(0)
            elif par_client[topic] <= 2:
                print(topic, json.dumps(ev, ensure_ascii=False))
        if producteur:
            producteur.flush(60)
        if lignes and not a.afficher:
            cur.execute("""INSERT INTO diffusion.filigrane VALUES ('evenements_clients', %s)
                           ON CONFLICT (canal) DO UPDATE SET dernier_maj = EXCLUDED.dernier_maj""", (lignes[-1][8],))
            cx.commit()
        for topic, n in sorted(par_client.items()):
            print(f"{topic} : {n} événements {'affichés' if a.afficher else 'publiés'}")
        if not lignes:
            print("Aucun changement depuis la dernière publication.")


if __name__ == "__main__":
    main()
