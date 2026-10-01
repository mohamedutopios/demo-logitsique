"""Génère l'activité simulée des systèmes sources d'un logisticien.

Écrit :
  - dans la base « sources » : WMS nord et sud, TMS, ticketing, portail DONUT ;
  - dans /depot : fichiers transporteurs (GEODIS en JSON, DPD en CSV),
    exports Kelio (temps de travail), fichiers EDI ORDERS du client CLI003 ;
  - dans Kafka (topic erp.commandes) : événements de l'ERP du client CLI004.

Utilisation :
  python generateur/generer.py --jours 60     # historique : 60 jours jusqu'à hier
  python generateur/generer.py --jour-suivant # un jour de plus après le dernier généré
"""
import argparse, csv, hashlib, json, math, os, random, sys
from datetime import date, datetime, time, timedelta
import psycopg

SOURCES_DSN = os.environ.get("SOURCES_DSN", "postgresql://sources:sources@sources-db:5432/sources")
DEPOT = os.environ.get("DEPOT", "/depot")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")

# client → (site, code dans le WMS du site, volume moyen de commandes par jour ouvré)
CLIENTS = [("LIL", "CLI001", 90), ("LIL", "CLI002", 60), ("LIL", "CLI003", 40),
           ("LYO", "C-003", 35), ("LYO", "C-004", 110), ("LYO", "C-005", 50)]
EFFECTIFS = {"LIL": 40, "LYO": 35}
CADENCES = {"RECEPTION": 12, "PREPARATION": 45, "EXPEDITION": 60}


def graine(*parts):
    """Aléa reproductible : même entrée → même tirage."""
    return random.Random(int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:12], 16))


def horodatage(jour, rnd, h_min=7, h_max=19):
    return datetime.combine(jour, time(rnd.randint(h_min, h_max - 1), rnd.randint(0, 59), rnd.randint(0, 59)))


def volume_du_jour(jour, moyenne, rnd):
    if jour.weekday() == 6:
        return 0
    base = moyenne * (0.35 if jour.weekday() == 5 else 1.0)
    saison = 1 + 0.15 * math.sin(jour.timetuple().tm_yday / 58.0)
    return max(0, int(rnd.gauss(base * saison, base * 0.15)))


def dernier_jour(cur):
    cur.execute("SELECT max(date_photo) FROM wms_nord.stock")
    return cur.fetchone()[0]


def generer_jour(cur, jour, producteur, bilan):
    rnd = graine("jour", jour)
    fin = datetime.combine(jour, time(20, 0))

    # 1. Commandes de la veille restées ouvertes : expédiées aujourd'hui
    cur.execute("""UPDATE wms_nord.commandes SET statut='EXPEDIEE', date_expedition=%s, maj_le=%s
                   WHERE statut='PREPAREE' RETURNING num_commande, code_client, nb_colis, date_creation""",
                (datetime.combine(jour, time(10, 30)), datetime.combine(jour, time(10, 30))))
    a_expedier = [("LIL", *r) for r in cur.fetchall()]
    cur.execute("""UPDATE wms_sud.outbound SET status=60, shipped_at=%s, updated_at=%s
                   WHERE status=40 RETURNING ref, customer, parcels, created_at""",
                (datetime.combine(jour, time(10, 45)), datetime.combine(jour, time(10, 45))))
    a_expedier += [("LYO", *r) for r in cur.fetchall()]
    cur.execute("""UPDATE wms_nord.receptions SET statut='CLOTUREE', date_reception=%s, maj_le=%s
                   WHERE statut='EN_COURS'""", (datetime.combine(jour, time(9, 0)), datetime.combine(jour, time(9, 0))))
    cur.execute("""UPDATE wms_sud.inbound SET status=30, received_at=%s, updated_at=%s
                   WHERE status=20""", (datetime.combine(jour, time(9, 15)), datetime.combine(jour, time(9, 15))))

    volumes = {s: {"RECEPTION": 0, "PREPARATION": 0, "EXPEDITION": 0} for s in EFFECTIFS}
    edi_lignes, evenements = [], []

    for site, code, moyenne in CLIENTS:
        r = graine(jour, site, code)
        # 2. Réceptions
        for k in range(0 if jour.weekday() >= 5 else r.randint(2, 7)):
            num = f"REC-{site}-{jour:%Y%m%d}-{code}-{k:02d}"
            palettes = r.randint(4, 26)
            en_retard = r.random() < 0.1
            t = horodatage(jour, r, 6, 12)
            volumes[site]["RECEPTION"] += 0 if en_retard else palettes
            if site == "LIL":
                cur.execute("""INSERT INTO wms_nord.receptions (num_reception, code_client, date_prevue, date_reception, statut, nb_palettes, maj_le)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            (num, code, jour, None if en_retard else t, "EN_COURS" if en_retard else "CLOTUREE", palettes, t))
            else:
                cur.execute("""INSERT INTO wms_sud.inbound (ref, customer, expected_at, received_at, status, pallets, updated_at)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            (num, code, jour, None if en_retard else t, 20 if en_retard else 30, palettes, t))
            bilan["receptions"] += 1

        # 3. Commandes
        for k in range(volume_du_jour(jour, moyenne, r)):
            num = f"CMD-{site}-{jour:%Y%m%d}-{code}-{k:04d}"
            lignes = r.randint(1, 8)
            colis = math.ceil(lignes / 3)
            cree = horodatage(jour, r, 7, 17)
            tirage = r.random()
            statut = "ANNULEE" if tirage < 0.02 else ("PREPAREE" if tirage < 0.30 else "EXPEDIEE")
            expedie = cree + timedelta(hours=r.randint(2, 5)) if statut == "EXPEDIEE" else None
            maj = expedie or cree + timedelta(hours=1)
            if statut != "ANNULEE":
                volumes[site]["PREPARATION"] += lignes
            if site == "LIL":
                cur.execute("""INSERT INTO wms_nord.commandes (num_commande, code_client, date_creation, date_expedition, statut, nb_lignes, nb_colis, maj_le)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (num, code, cree, expedie, statut, lignes, colis, maj))
            else:
                cur.execute("""INSERT INTO wms_sud.outbound (ref, customer, created_at, shipped_at, status, lines, parcels, updated_at)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (num, code, cree, expedie, {"ANNULEE": 90, "PREPAREE": 40, "EXPEDIEE": 60}[statut], lignes, colis, maj))
            if statut == "EXPEDIEE":
                a_expedier.append((site, num, code, colis, cree))
            if code == "CLI003":
                for n in range(lignes):
                    edi_lignes.append({"num_commande": num, "sku": f"BIO-{r.randint(1, 400):04d}",
                                       "quantite": r.randint(1, 24), "date_souhaitee": f"{jour + timedelta(days=1):%Y-%m-%d}"})
            if code == "C-004" and r.random() > 0.01:   # 1 % d'événements perdus : la qualité doit le voir
                evenements.append({"client": "CLI004", "num_commande": num, "evenement": "COMMANDE_CREEE",
                                   "horodatage": cree.isoformat(), "nb_lignes": lignes})
            bilan["commandes"] += 1

        # 4. Photo de stock quotidienne (20 références par client)
        for n in range(20):
            sku = f"{code}-SKU{n:03d}"
            qte = max(0, int(500 + 300 * math.sin((jour.toordinal() + n) / 9) + graine(jour, code, n).randint(-40, 40)))
            if site == "LIL":
                cur.execute("INSERT INTO wms_nord.stock (code_client, sku, quantite, date_photo, maj_le) VALUES (%s,%s,%s,%s,%s)",
                            (code, sku, qte, jour, fin))
            else:
                cur.execute("INSERT INTO wms_sud.inventory (customer, item, qty, snapshot_date, updated_at) VALUES (%s,%s,%s,%s,%s)",
                            (code, sku, qte, jour, fin))

    # 5. Expéditions dans le TMS
    for site, num, code, colis, cree in a_expedier:
        r = graine("exp", num)
        poids = round(colis * r.uniform(3, 15), 2)
        depart = max(cree + timedelta(hours=3), datetime.combine(jour, time(11, 0)))
        transporteur = "GEODIS" if r.random() < 0.55 else "DPD"
        cur.execute("""INSERT INTO tms.expeditions (num_expedition, num_commande, site, code_client, transporteur, date_depart, poids_kg, nb_colis, cout_ht, maj_le)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (num.replace("CMD", "EXP"), num, site, code, transporteur, depart, poids, colis,
                     round(6.5 + 0.62 * poids, 2), depart))
        volumes[site]["EXPEDITION"] += colis
        bilan["expeditions"] += 1

    # 6. Tickets : ouverture et résolution
    cur.execute("SELECT id FROM ticketing.tickets WHERE statut='OUVERT'")
    for (tid,) in cur.fetchall():
        if graine("resol", tid, jour).random() < 0.6:
            t = horodatage(jour, graine("t", tid, jour))
            cur.execute("UPDATE ticketing.tickets SET statut='RESOLU', resolu_le=%s, maj_le=%s WHERE id=%s", (t, t, tid))
    for n in range(rnd.randint(0, 4)):
        site, code, _ = CLIENTS[rnd.randrange(len(CLIENTS))]
        client = {"C-003": "CLI003", "C-004": "CLI004", "C-005": "CLI005"}.get(code, code)
        t = horodatage(jour, rnd)
        cur.execute("""INSERT INTO ticketing.tickets (code_client, site, categorie, priorite, ouvert_le, statut, maj_le)
                       VALUES (%s,%s,%s,%s,%s,'OUVERT',%s)""",
                    (client, site, rnd.choice(["LITIGE", "RETARD", "CASSE", "QUESTION"]), rnd.choice(["P1", "P2", "P2", "P3"]), t, t))

    # 7. Saisies manuelles DONUT (prestations annexes)
    for n in range(rnd.randint(0, 3)):
        client = rnd.choice(["CLI001", "CLI002", "CLI003", "CLI004", "CLI005"])
        t = horodatage(jour, rnd)
        cur.execute("""INSERT INTO donut.saisies (code_client, type_saisie, reference, valeur, saisi_le, maj_le)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    (client, rnd.choice(["ETIQUETAGE", "REEMBALLAGE"]), f"DON-{jour:%Y%m%d}-{n}", rnd.randint(20, 400), t, t))

    ecrire_fichiers(cur, jour, volumes, edi_lignes)
    if producteur is not None:
        for ev in evenements:
            producteur.produce("erp.commandes", key=ev["num_commande"], value=json.dumps(ev))
        producteur.flush(30)
    bilan["evenements"] += len(evenements)


def evenements_transport(num_expedition, transporteur, depart):
    """Suivi transporteur reproductible d'une expédition (horodatages éventuellement futurs)."""
    r = graine("suivi", num_expedition)
    evs = [("PRIS_EN_CHARGE", depart + timedelta(hours=1, minutes=r.randint(0, 50)))]
    evs.append(("EN_TRANSIT", depart + timedelta(hours=r.randint(8, 14))))
    if r.random() < 0.05:
        evs.append(("INCIDENT", depart + timedelta(hours=r.randint(20, 30))))
    evs.append(("LIVRE", depart + timedelta(hours=r.randint(22, 52))))
    codes = {"PRIS_EN_CHARGE": "PICKUP", "EN_TRANSIT": "TRANSIT", "INCIDENT": "EXCEPTION", "LIVRE": "DELIVERED"}
    return [(codes[s] if transporteur == "DPD" else s, t) for s, t in evs]


def ecrire_fichiers(cur, jour, volumes, edi_lignes):
    os.makedirs(f"{DEPOT}/transporteurs", exist_ok=True)
    os.makedirs(f"{DEPOT}/kelio", exist_ok=True)
    os.makedirs(f"{DEPOT}/edi", exist_ok=True)
    # Transporteurs : événements dont l'horodatage tombe ce jour-là
    cur.execute("""SELECT num_expedition, transporteur, date_depart FROM tms.expeditions
                   WHERE date_depart >= %s AND date_depart < %s""", (jour - timedelta(days=3), jour + timedelta(days=1)))
    geodis, dpd = [], []
    for num, tr, depart in cur.fetchall():
        for statut, t in evenements_transport(num, tr, depart):
            if t.date() == jour:
                if tr == "GEODIS":
                    geodis.append({"expedition": num, "statut": statut, "horodatage": t.isoformat(), "lieu": "Plateforme"})
                else:
                    dpd.append([num, statut, t.strftime("%d/%m/%Y %H:%M"), "Hub DPD"])
    with open(f"{DEPOT}/transporteurs/geodis_{jour:%Y%m%d}.json", "w", encoding="utf-8") as f:
        json.dump(geodis, f, ensure_ascii=False)
    with open(f"{DEPOT}/transporteurs/dpd_{jour:%Y%m%d}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["ref_expedition", "code_evenement", "date_heure", "ville"])
        w.writerows(dpd)
    # Kelio : heures par salarié et par activité, cohérentes avec les volumes
    with open(f"{DEPOT}/kelio/temps_{jour:%Y%m%d}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["matricule", "site", "date", "activite", "heures"])
        for site, effectif in EFFECTIFS.items():
            r = graine("kelio", site, jour)
            besoins = {a: volumes[site][a] / (CADENCES[a] * r.uniform(0.85, 1.1)) for a in CADENCES}
            m = 0
            for activite, heures in besoins.items():
                while heures > 0.5 and m < effectif:
                    h = round(min(7.5, heures) * 4) / 4
                    w.writerow([f"{site}{m:04d}", site, f"{jour:%Y-%m-%d}", activite, f"{h:.2f}".replace(".", ",")])
                    heures -= h
                    m += 1
            if m < effectif and jour.weekday() < 5:
                for k in range(m, min(effectif, m + 3)):
                    w.writerow([f"{site}{k:04d}", site, f"{jour:%Y-%m-%d}", "ADMIN", "7,00"])
    # EDI ORDERS du client CLI003
    with open(f"{DEPOT}/edi/ORDERS_CLI003_{jour:%Y%m%d}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["num_commande", "sku", "quantite", "date_souhaitee"], delimiter=";")
        w.writeheader()
        w.writerows(edi_lignes)


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--jours", type=int, help="nombre de jours d'historique à générer, jusqu'à hier")
    g.add_argument("--jour-suivant", action="store_true", help="générer le jour qui suit le dernier jour généré")
    p.add_argument("--sans-kafka", action="store_true", help="ne pas publier les événements ERP (Redpanda arrêté)")
    a = p.parse_args()

    producteur = None
    if not a.sans_kafka:
        from confluent_kafka import Producer
        producteur = Producer({"bootstrap.servers": KAFKA, "acks": "all"})

    with psycopg.connect(SOURCES_DSN) as cx, cx.cursor() as cur:
        dernier = dernier_jour(cur)
        if a.jours:
            if dernier is not None:
                sys.exit("Des données existent déjà : utilisez --jour-suivant, ou réinitialisez (voir README).")
            jours = [date.today() - timedelta(days=a.jours - i) for i in range(a.jours)]
        else:
            if dernier is None:
                sys.exit("Aucune donnée : lancez d'abord --jours 60.")
            jours = [dernier + timedelta(days=1)]
        bilan = {"receptions": 0, "commandes": 0, "expeditions": 0, "evenements": 0}
        for j in jours:
            generer_jour(cur, j, producteur, bilan)
            cx.commit()
        print(f"Jours générés : {jours[0]} → {jours[-1]} ({len(jours)} j)")
        print(f"Réceptions {bilan['receptions']} · commandes {bilan['commandes']} · "
              f"expéditions {bilan['expeditions']} · événements ERP {bilan['evenements']}")


if __name__ == "__main__":
    main()
