"""Charge la zone brute (schéma raw) de l'entrepôt.

  1. Bases sources : extraction incrémentale (date de modification > filigrane).
  2. Fichiers déposés dans /depot : chaque fichier est chargé une seule fois puis archivé.
  3. Kafka : messages du topic erp.commandes, offsets validés après écriture.

Utilisation : python chargeur/charger.py [--sans-kafka]
"""
import argparse, csv, json, os, shutil, sys
from datetime import datetime
import psycopg

SOURCES_DSN = os.environ.get("SOURCES_EXTRACT_DSN", "postgresql://extraction:extraction@sources-db:5432/sources")
DWH_DSN = os.environ.get("DWH_CHARGEUR_DSN", "postgresql://chargeur:chargeur@dwh:5432/dwh")
DEPOT = os.environ.get("DEPOT", "/depot")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")

# source, table, expression de la clé, colonne de date de modification
TABLES = [
    ("wms_nord", "receptions", "num_reception", "maj_le"),
    ("wms_nord", "commandes", "num_commande", "maj_le"),
    ("wms_nord", "stock", "code_client || '|' || sku || '|' || date_photo", "maj_le"),
    ("wms_sud", "inbound", "ref", "updated_at"),
    ("wms_sud", "outbound", "ref", "updated_at"),
    ("wms_sud", "inventory", "customer || '|' || item || '|' || snapshot_date", "updated_at"),
    ("tms", "expeditions", "num_expedition", "maj_le"),
    ("ticketing", "tickets", "id::text", "maj_le"),
    ("donut", "saisies", "id::text", "maj_le"),
]
FICHIERS = [("transporteurs", "geodis_", "transporteur_geodis"), ("transporteurs", "dpd_", "transporteur_dpd"),
            ("kelio", "temps_", "kelio"), ("edi", "ORDERS_", "edi_orders")]


def charger_bases(dwh):
    total = 0
    with psycopg.connect(SOURCES_DSN) as src, src.cursor() as s, dwh.cursor() as d:
        for source, table, cle, maj in TABLES:
            d.execute("SELECT dernier_maj FROM raw.filigrane WHERE source=%s AND table_source=%s", (source, table))
            row = d.fetchone()
            filigrane = row[0] if row else datetime(1900, 1, 1)
            s.execute(f"SELECT {cle}, {maj}, row_to_json(t)::text FROM {source}.{table} t "
                      f"WHERE {maj} > %s ORDER BY {maj}", (filigrane,))
            lignes = s.fetchall()
            if not lignes:
                continue
            with d.copy("COPY raw.extraction (source, table_source, cle, maj_source, payload) FROM STDIN") as cp:
                for k, m, payload in lignes:
                    cp.write_row((source, table, k, m, payload))
            d.execute("""INSERT INTO raw.filigrane VALUES (%s,%s,%s)
                         ON CONFLICT (source, table_source) DO UPDATE SET dernier_maj = EXCLUDED.dernier_maj""",
                      (source, table, lignes[-1][1]))
            dwh.commit()
            print(f"  {source}.{table:<11} {len(lignes):>7} lignes (filigrane {lignes[-1][1]})")
            total += len(lignes)
    return total


def lire(chemin):
    if chemin.endswith(".json"):
        with open(chemin, encoding="utf-8") as f:
            return json.load(f)
    with open(chemin, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def charger_fichiers(dwh):
    total = 0
    with dwh.cursor() as d:
        for dossier, prefixe, type_fichier in FICHIERS:
            rep = os.path.join(DEPOT, dossier)
            if not os.path.isdir(rep):
                continue
            for nom in sorted(f for f in os.listdir(rep) if f.startswith(prefixe)):
                chemin = os.path.join(rep, nom)
                d.execute("SELECT 1 FROM raw.fichiers_charges WHERE nom_fichier=%s", (nom,))
                if d.fetchone() is None:
                    lignes = lire(chemin)
                    with d.copy("COPY raw.fichiers (type_fichier, nom_fichier, num_ligne, payload) FROM STDIN") as cp:
                        for i, l in enumerate(lignes, 1):
                            cp.write_row((type_fichier, nom, i, json.dumps(l, ensure_ascii=False)))
                    d.execute("INSERT INTO raw.fichiers_charges (nom_fichier, type_fichier, nb_lignes) VALUES (%s,%s,%s)",
                              (nom, type_fichier, len(lignes)))
                    dwh.commit()
                    total += len(lignes)
                archive = os.path.join(DEPOT, "archive", dossier)
                os.makedirs(archive, exist_ok=True)
                shutil.move(chemin, os.path.join(archive, nom))
    return total


def charger_kafka(dwh):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "chargeur-dwh",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(["erp.commandes"])
    total, vides = 0, 0
    try:
        with dwh.cursor() as d:
            while vides < 5:
                msgs = c.consume(num_messages=500, timeout=1.0)
                if not msgs:
                    vides += 1
                    continue
                vides = 0
                for m in msgs:
                    if m.error():
                        raise RuntimeError(m.error())
                    d.execute("""INSERT INTO raw.evenements (topic, partition, position, cle, payload)
                                 VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                              (m.topic(), m.partition(), m.offset(),
                               m.key().decode() if m.key() else None, m.value().decode()))
                dwh.commit()
                c.commit(asynchronous=False)   # l'offset n'avance qu'une fois les lignes écrites
                total += len(msgs)
    finally:
        c.close()
    return total


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sans-kafka", action="store_true")
    a = p.parse_args()
    with psycopg.connect(DWH_DSN) as dwh:
        print("Bases sources :")
        n1 = charger_bases(dwh)
        n2 = charger_fichiers(dwh)
        print(f"Fichiers : {n2} lignes")
        n3 = 0 if a.sans_kafka else charger_kafka(dwh)
        print(f"Événements Kafka : {n3}")
    print(f"Total chargé dans raw : {n1 + n2 + n3} lignes")


if __name__ == "__main__":
    sys.exit(main())
