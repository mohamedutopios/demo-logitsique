# Kit local — Data Warehouse d'un logisticien (WMS, TMS, transporteurs, Kelio, ticketing)

Ce kit fait tourner **en local, dans des conteneurs**, l'architecture du schéma projet :

- des sources simulées ;
- un entrepôt de données historisé et contrôlé ;
- les consommateurs internes (reporting, IA) et clients (exports, portail, événements).

Tout est reproductible, sans compte cloud. Une fois les images téléchargées, aucune connexion Internet n'est nécessaire.

Chaque étape du parcours indique :
- **ce que fait la commande** ;
- **comment vérifier** que tout s'est bien passé, avec le résultat attendu ;
- **où le voir** dans une interface graphique.

---

## 1. Quel entrepôt, et pourquoi

**Choix du kit : PostgreSQL 17 comme entrepôt, dbt pour les transformations et les tests.**

| Besoin du schéma | Ce qu'il impose | Pourquoi PostgreSQL y répond |
|---|---|---|
| Historique | garder toutes les versions des lignes sources | zone brute append-only (`raw.extraction`), rejouable |
| Qualité | tests bloquants, rapprochements entre sources | dbt : tests + table `mart_qualite_anomalies` |
| Portail de référentiels | écritures transactionnelles (tarifs, correspondances) | base transactionnelle ACID native |
| Portail client | milliers de petites requêtes, chaque client ne voit que ses lignes | index, vues filtrées par jeton JWT, API PostgREST |
| Qlik, Power BI | connecteurs natifs | connecteur PostgreSQL présent dans les deux outils |
| Volumes d'un logisticien | quelques dizaines de millions de lignes par an | largement dans la zone de confort d'un seul serveur |
| Coût, souveraineté | sur site ou chez n'importe quel cloud | open source, sans licence |

**Quand changer :**
- **ClickHouse** (open source, sur site) pour les agrégats et le portail : si les volumes dépassent quelques To actifs, ou si des centaines de tableaux de bord interrogent en même temps.
- **Fabric ou Snowflake** si l'organisation est déjà massivement Power BI / Azure, ou multi-cloud.
- **BigQuery** si le SI est sur Google Cloud.

Les modèles dbt se portent en changeant d'adaptateur. Quelques fonctions propres à PostgreSQL sont à réécrire : `distinct on`, `->>`, `generate_series`, `to_timestamp`.

---

## 2. Correspondance avec le schéma du projet

| Boîte du schéma | Dans le kit | Conteneur |
|---|---|---|
| Nos différents WMS | deux WMS d'éditeurs différents : `wms_nord` (Lille, codes `CLI001`, statuts en texte) et `wms_sud` (Lyon, codes `C-001`, statuts numériques) | `sources-db` |
| Nos différents TMS | `tms.expeditions` | `sources-db` |
| Nos différents transporteurs | fichiers de suivi GEODIS (JSON) et DPD (CSV), formats et codes différents | dossier `depot/transporteurs` |
| KELIO gestion des temps | export CSV quotidien des heures par salarié et activité | dossier `depot/kelio` |
| Ticketing | `ticketing.tickets` | `sources-db` |
| ERP/CMS clients — EDI | fichiers EDI ORDERS du client CLI003 | dossier `depot/edi` |
| ERP/CMS clients — EVT | événements `COMMANDE_CREEE` de l'ERP du client CLI004 | topic Kafka `erp.commandes` (Redpanda) |
| Portail Web DONUT | `donut.saisies` (prestations annexes saisies à la main) | `sources-db` |
| **Data Warehouse (Historique / Qualité)** | PostgreSQL : `raw` → `staging` → `core` → `marts` → `api`, tests dbt | `dwh` |
| Portail d'injection de référentiels externes | NocoDB branché sur le schéma `ref` | `nocodb` |
| Visualisation (Qlik, Power BI) | Metabase ; Power BI Desktop ou Qlik se connectent aussi directement | `metabase` |
| IA (prédictions) | prévision des commandes à 7 jours (scikit-learn) | `outils` → `ia.previsions_commandes` |
| Reportings logistique | `marts.mart_logistique_jour`, `marts.mart_facturation_mois` | `dwh` |
| Reporting RH | `marts.mart_rh_temps` | `dwh` |
| Contrôle de gestion, productivité | `marts.mart_productivite` (unités par heure contre objectif) | `dwh` |
| Export de données (CSV) | un dossier par client | dossier `exports/` |
| Portail client | API REST filtrée par client (jeton JWT) | `postgrest` |
| Événements auxquels s'abonner | un topic par client : `clients.<code>.expeditions` | `redpanda` |

---

## 3. Architecture

```
 SOURCES                          CHARGEMENT                ENTREPÔT (PostgreSQL « dwh »)                 CONSOMMATEURS
┌──────────────────────┐                                 ┌──────────────────────────────────┐
│ sources-db           │  extraction incrémentale        │ raw      tout, jamais modifié     │     ┌──────────────────────┐
│  wms_nord, wms_sud   │ ─────────────┐                  │  └ extraction, fichiers,          │ ──▶ │ Metabase / Power BI  │
│  tms, ticketing,     │              │                  │    evenements                     │     │ (reportings)         │
│  donut               │              ▼                  │ ref      référentiels ◀── NocoDB   │     └──────────────────────┘
└──────────────────────┘      ┌──────────────┐  dbt      │ staging  harmonisation (codes,     │     ┌──────────────────────┐
┌──────────────────────┐      │ chargeur     │ ───────▶  │          statuts, formats)         │ ──▶ │ IA : prévisions      │
│ depot/  (SFTP simulé)│ ───▶ │ Python       │           │ core     faits et dimensions       │     └──────────────────────┘
│  transporteurs, kelio│      │ (conteneur   │           │ marts    reportings, qualité,      │     ┌──────────────────────┐
│  edi                 │      │  outils)     │           │          suivi client              │ ──▶ │ exports/<client>/*.csv│
└──────────────────────┘      └──────────────┘           │ api      vues filtrées par client  │     └──────────────────────┘
┌──────────────────────┐              ▲                  └──────────────────────────────────┘     ┌──────────────────────┐
│ redpanda (Kafka)     │ ─────────────┘                          │                        │ ──▶ │ PostgREST :3001      │
│  erp.commandes       │ ◀────────────────── clients.<code>.expeditions ◀─────────────────┘     │ (portail client)     │
└──────────────────────┘                                                                          └──────────────────────┘
```

| Conteneur | Rôle | Port sur le poste |
|---|---|---|
| `sources-db` | bases sources simulées (PostgreSQL 17) | 55433 |
| `dwh` | entrepôt (PostgreSQL 17) | 55432 |
| `redpanda` | bus d'événements compatible Kafka | 19092 |
| `redpanda-console` | voir topics, messages, groupes de consommateurs | 8080 |
| `pgadmin` | **explorer les bases graphiquement** (entrepôt et sources) | 5050 |
| `dbt-docs` | documentation et **graphe de lignage** dbt | 8083 |
| `metabase` | tableaux de bord | 3000 |
| `nocodb` | portail des référentiels | 8081 |
| `postgrest` | API du portail client | 3001 |
| `config-interfaces` | prépare Metabase et NocoDB au démarrage (comptes, connexions), puis s'arrête | — |
| `outils` | générateur, chargeur, dbt, exports, événements, IA (lancé à la demande) | — |

### Les interfaces pour suivre chaque étape

| Adresse | Pour voir | Identifiants |
|---|---|---|
| http://localhost:5050 | **pgAdmin** : tables et données de toutes les couches, requêtes SQL, schéma relationnel | aucun (connexions préconfigurées) |
| http://localhost:8080 | **Redpanda Console** : topics Kafka, messages, retard du chargeur | aucun |
| http://localhost:8083 | **Documentation dbt** : modèles, colonnes, tests, graphe de lignage | aucun (après `dbt docs generate`) |
| http://localhost:3000 | **Metabase** : tableaux de bord, entrepôt déjà connecté (`lecture_bi`) | `admin@example.com` / `Admin-Kit-2026` |
| http://localhost:8081 | **NocoDB** : édition des référentiels comme un tableur, base « Référentiels » déjà branchée (`portail_ref`) | `admin@example.com` / `Admin-Kit-2026` |
| http://localhost:3001 | **PostgREST** : API du portail client | jeton JWT (section 13) |
| dossiers `depot/` et `exports/` | fichiers déposés et produits | Finder / Explorateur |

**pgAdmin** s'ouvre sans mot de passe sur trois connexions, regroupées sous « Kit DWH logistique » :

| Connexion | Utilisateur | Ce qu'on y voit |
|---|---|---|
| `1 · Entrepôt dwh (dwh_admin…)` | `dwh_admin` | tous les schémas : `raw`, `ref`, `staging`, `core`, `marts`, `api`, `ia`, `diffusion` |
| `2 · Entrepôt dwh (lecture_bi…)` | `lecture_bi` | le point de vue des outils BI : `core`, `marts`, `ref`, `ia` (accès refusé à `raw`) |
| `3 · Sources simulées…` | `sources` | les bases « opérationnelles » : `wms_nord`, `wms_sud`, `tms`, `ticketing`, `donut` |

Mode d'emploi :
- **Parcourir une table** : déplier *Databases → dwh → Schemas → raw → Tables*, clic droit sur une table → *View/Edit Data → First 100 Rows*.
- **Lancer une requête** : sélectionner la base, puis *Tools → Query Tool*, coller une requête de ce README et appuyer sur F5.
- **Voir le modèle de données** : clic droit sur la base `dwh` → *ERD For Database*.

---

## 4. Ce qui se passe dans l'entrepôt

L'entrepôt est organisé en **couches**. Chaque couche ne lit que la précédente : en cas d'erreur, on sait où regarder,
et on peut tout reconstruire à partir de la zone brute.

```
 sources ──chargeur──▶ raw ──dbt──▶ staging ──dbt──▶ core ──dbt──▶ marts ──dbt──▶ api
                        │           (vues)          (tables)       (tables)       (vues)
                        │              ▲
                        └── ref ───────┘  (correspondances de codes, statuts, tarifs)
```

| Couche | Créée par | Type | Contenu | Règle |
|---|---|---|---|---|
| `raw` | `dwh/01_init_dwh.sql`, remplie par le **chargeur** | tables | copie fidèle de tout ce qui arrive : lignes des bases (en JSON), lignes de fichiers, messages Kafka | **on ajoute, on ne modifie jamais**. Une commande modifiée trois fois existe en trois versions. |
| `ref` | init SQL, puis **NocoDB** | tables | clients, sites, correspondance des codes clients, traduction des statuts, tarifs, objectifs | la seule couche modifiée à la main |
| `staging` | **dbt** | vues | une vue par type de donnée, qui **harmonise** les sources : dernière version de chaque ligne, codes traduits via `ref`, formats de date unifiés | aucune règle métier, seulement de la mise en forme |
| `core` | **dbt** | tables | **faits** (`fct_commandes`, `fct_expeditions`, `fct_receptions`, `fct_temps_travail`…) et **dimensions** (`dim_client`, `dim_site`, `dim_date`) | une ligne = un objet métier ; **tests** d'unicité, de nullité, de référence, de valeurs |
| `marts` | **dbt** | tables | tables prêtes à consommer : reportings, facturation, productivité, qualité, suivi client | ce que lisent Metabase, Power BI, l'IA, les exports |
| `api` | **dbt** | vues | `v_expeditions`, `v_receptions`, `v_stock` : filtrées sur le client du jeton JWT | seule porte d'entrée du portail client |
| `ia` | script IA | table | `previsions_commandes` | |
| `diffusion` | init SQL | table | `filigrane` : jusqu'où les événements clients ont été publiés | |

### Le parcours d'une commande, de la source au portail

1. **Source.** Le WMS de Lyon enregistre `CMD-LYO-…-C-004-…`, client `C-004`, statut `40` (code de l'éditeur).
2. **raw.extraction.** Le chargeur copie la ligne entière en JSON (`payload`), avec sa date de modification. Le
   lendemain, la commande passe au statut `60` : une **deuxième** ligne est ajoutée, la première reste.
3. **staging.stg_commandes.** dbt ne garde que la dernière version (`distinct on … order by maj_source desc`) et
   traduit les codes avec `ref` : `C-004` devient `CLI004`, `60` devient `EXPEDIEE`. Les commandes du WMS de Lille,
   qui a ses propres codes, arrivent au même format : les deux WMS sont réunis.
4. **core.fct_commandes.** dbt calcule le délai d'expédition et l'indicateur « expédiée le jour même ». Les tests
   vérifient que la commande est unique et a un client et un statut connus.
5. **marts.** `mart_logistique_jour` la compte dans les volumes du jour, `mart_facturation_mois` la valorise au tarif
   en vigueur, `mart_client_expeditions` la joint à son expédition et à son suivi transporteur.
6. **api.v_expeditions.** Le portail ne la montre qu'à CLI004.

### dbt en bref

`dbt build` lit les fichiers SQL de `dbt/models/`, déduit l'ordre d'exécution des `ref()` (le graphe de lignage),
puis, dans cet ordre :
- crée ou remplace chaque vue (`staging`, `api`) ou table (`core`, `marts`) ;
- exécute les **tests** du modèle juste après sa construction. Un test en échec bloque tout ce qui en dépend
  (`SKIP`) : les données fausses ne se propagent pas ;
- termine par les **droits** (`on-run-end` dans `dbt_project.yml`) : lecture de `core` et `marts` pour `lecture_bi`,
  lecture de `api` pour le portail.

Les 60 éléments d'un `dbt build` complet : 30 modèles, 26 tests, 4 opérations de droits.

### Qui a le droit de quoi

| Rôle PostgreSQL | Utilisé par | Peut |
|---|---|---|
| `dwh_admin` | dbt, IA, publication d'événements, administration | tout |
| `chargeur` | chargeur Python | écrire dans `raw` uniquement |
| `lecture_bi` | Metabase, Power BI, Qlik, exports CSV | lire `core`, `marts`, `ref`, `ia` ; **pas** `raw` ni `staging` |
| `portail_ref` | NocoDB | modifier `ref` |
| `authenticator` → `portail_client` | PostgREST, avec jeton | lire les vues `api`, filtrées sur son client |
| `authenticator` → `web_anon` | PostgREST, sans jeton | rien |

Côté sources, le chargeur se connecte avec le rôle `extraction`, en **lecture seule**.

---

## 5. Étape 0 — Prérequis

| Élément | Minimum | Vérification |
|---|---|---|
| Docker Desktop (macOS, Windows avec WSL 2) ou Docker Engine (Linux) | Docker Compose v2.20 ou plus | `docker compose version` |
| Mémoire allouée à Docker | 6 Go | Docker Desktop → Settings → Resources |
| Disque libre | 5 Go | — |
| Ports libres | 3000, 3001, 5050, 8080, 8081, 8083, 19092, 55432, 55433 | voir Dépannage |
| Terminal bash ou zsh | macOS (zsh par défaut), Linux, WSL ou Git Bash | les commandes utilisent `$( )` et `\`. Gardez les guillemets autour des URL : zsh interprète `?` et `&` |

Sur Mac Apple Silicon, toutes les images existent en `linux/arm64`. Toutes les commandes se lancent depuis le
dossier du kit.

---

## 6. Étape 1 — Installer et démarrer

```bash
unzip kit-dwh-logistique-v1_0.zip
cd kit-dwh-logistique-v1_0
docker compose build outils
docker compose up -d
docker compose ps
```

**Ce que fait `build`.** Il construit l'image `outils` : Python 3.12, psycopg, confluent-kafka, pandas,
scikit-learn, dbt-core 1.11 et dbt-postgres 1.11. Comptez 2 à 4 minutes.

**Ce que fait `up -d`.** Au premier démarrage :
- `sources-db` exécute `sources/01_schema_sources.sql` : schémas et tables **vides** des sources, et rôle
  `extraction` en lecture seule ;
- `dwh` exécute `dwh/01_init_dwh.sql` : zones `raw`, `ref`, `ia`, `diffusion`, **référentiels remplis** et rôles ;
- `redpanda-init` crée les topics puis s'arrête (`Exited (0)` dans `docker compose ps -a`, c'est normal) ;
- `pgadmin`, `dbt-docs`, `metabase`, `nocodb`, `postgrest` et `redpanda-console` démarrent ;
- `config-interfaces` prépare Metabase et NocoDB, puis s'arrête (`Exited (0)`, c'est normal) :
  - **Metabase** : compte administrateur créé, entrepôt ajouté avec le rôle de lecture `lecture_bi` ;
  - **NocoDB** : compte administrateur créé, base « Référentiels (entrepôt dwh) » branchée sur le schéma `ref` avec
    le rôle `portail_ref`.

  Metabase met 1 à 2 minutes à démarrer : `config-interfaces` l'attend. Identifiants des deux outils :
  `admin@example.com` / `Admin-Kit-2026`.

Le projet Compose s'appelle `dwh-logistique-v1_0` : ses conteneurs et volumes sont distincts de ceux des autres
versions du kit. Deux versions ne peuvent pas tourner en même temps (mêmes ports) : arrêtez l'une
(`docker compose stop`) avant de démarrer l'autre.

### Vérifier

```bash
docker compose ps
```
Attendu : `sources-db`, `dwh` et `redpanda` en `(healthy)`, les autres en `Up`, sauf `redpanda-init` et
`config-interfaces`, qui finissent en `Exited (0)` (visibles avec `docker compose ps -a`).

```bash
docker compose logs config-interfaces
```
Attendu, en dernière ligne : `Interfaces prêtes. Identifiants Metabase et NocoDB : admin@example.com / Admin-Kit-2026`.

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "\dn"
```
Attendu : `diffusion`, `ia`, `public`, `raw`, `ref`. Les schémas `staging`, `core`, `marts` et `api` n'existent pas
encore : ils seront créés par dbt.

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT 'clients' AS referentiel, count(*) FROM ref.clients
UNION ALL SELECT 'sites', count(*) FROM ref.sites
UNION ALL SELECT 'correspondance_clients', count(*) FROM ref.correspondance_clients
UNION ALL SELECT 'statuts', count(*) FROM ref.statuts
UNION ALL SELECT 'tarifs', count(*) FROM ref.tarifs
UNION ALL SELECT 'objectifs_productivite', count(*) FROM ref.objectifs_productivite"
```
Attendu : 5 clients, 2 sites, 6 correspondances, 22 statuts, 25 tarifs (5 clients × 5 prestations), 6 objectifs.

```bash
docker compose exec redpanda rpk topic list
```
Attendu : `erp.commandes` (3 partitions) et `clients.CLI001.expeditions` à `clients.CLI005.expeditions`.

### Voir
- **pgAdmin** (http://localhost:5050) → connexion 1 → *Schemas → ref → Tables → tarifs* → *View/Edit Data*.
- **pgAdmin**, connexion 3 : les tables des sources existent mais sont vides.
- **Redpanda Console** (http://localhost:8080) → *Topics* : les 6 topics, sans message.

---

## 7. Étape 2 — Générer 60 jours d'activité

```bash
docker compose run --rm outils python generateur/generer.py --jours 60
```

Résultat attendu (les chiffres varient légèrement selon la date du jour) :

```
Jours générés : … → … (60 j)
Réceptions ~1 190 · commandes ~15 100 · expéditions ~14 700 · événements ERP ~4 260
```

Chaque `docker compose run` affiche d'abord quelques lignes `Container … Started` (démarrage des dépendances, dont
`redpanda-init` qui vérifie les topics) : c'est normal, le résultat de la commande vient ensuite.

### Ce que fait `generateur/generer.py`

Il simule, **jour par jour**, les systèmes d'un logisticien, sans rien écrire dans l'entrepôt. Pour chaque jour :

1. **Commandes de la veille restées « préparées »** : il les passe à « expédiée » (`UPDATE`, avec une nouvelle date
   de modification).
2. **Réceptions** fournisseurs : 10 % en retard. Lille les écrit dans `wms_nord.receptions`, Lyon dans `wms_sud.inbound`.
3. **Commandes** de chaque client, avec un volume qui varie selon le jour de la semaine et la saison :
   - 70 % sont expédiées le jour même, 28 % restent « préparées » jusqu'au lendemain, 2 % sont annulées ;
   - Lille écrit dans `wms_nord.commandes` (codes `CLI001`, statuts en texte) ; Lyon écrit dans `wms_sud.outbound`
     (codes `C-003`, statuts numériques `10/40/60/90`) ;
   - pour CLI003, il prépare les lignes du fichier EDI ;
   - pour CLI004 (à Lyon), il prépare un événement Kafka `COMMANDE_CREEE`, mais en « perd » volontairement 1 % pour
     que la qualité le détecte.
4. **Photo de stock** : 20 références par client et par jour.
5. **Expéditions** dans le TMS (`tms.expeditions`) : transporteur GEODIS ou DPD, poids, coût.
6. **Tickets** : ouvertures et résolutions.
7. **Saisies DONUT** : prestations annexes (étiquetage, réemballage).
8. **Fichiers** dans `depot/` :
   - suivi transporteur : GEODIS en JSON (`LIVRE`), DPD en CSV (`DELIVERED`, date `JJ/MM/AAAA`), avec 5 % d'incidents ;
   - export Kelio : heures par salarié et activité, cohérentes avec les volumes ;
   - EDI ORDERS de CLI003.
9. **Kafka** : publication des événements ERP dans `erp.commandes`.

Les tirages aléatoires dépendent de la date : relancer le kit à la même date redonne les mêmes données.

### Vérifier

Les tables sources sont remplies :

```bash
docker compose exec sources-db psql -U sources -d sources -c "
SELECT 'wms_nord.receptions' AS table_source, count(*) AS lignes FROM wms_nord.receptions
UNION ALL SELECT 'wms_nord.commandes', count(*) FROM wms_nord.commandes
UNION ALL SELECT 'wms_nord.stock', count(*) FROM wms_nord.stock
UNION ALL SELECT 'wms_sud.inbound', count(*) FROM wms_sud.inbound
UNION ALL SELECT 'wms_sud.outbound', count(*) FROM wms_sud.outbound
UNION ALL SELECT 'wms_sud.inventory', count(*) FROM wms_sud.inventory
UNION ALL SELECT 'tms.expeditions', count(*) FROM tms.expeditions
UNION ALL SELECT 'ticketing.tickets', count(*) FROM ticketing.tickets
UNION ALL SELECT 'donut.saisies', count(*) FROM donut.saisies"
```
Attendu (ordre de grandeur) : ~590 / ~7 480 / 3 600 / ~600 / ~7 630 / 3 600 / ~14 700 / ~130 / ~80. Les stocks
font exactement 3 600 lignes : 60 jours × 3 clients × 20 références.

Les deux WMS parlent des langues différentes (c'est ce que l'entrepôt devra harmoniser) :

```bash
docker compose exec sources-db psql -U sources -d sources -c "SELECT code_client, statut, count(*) FROM wms_nord.commandes GROUP BY 1, 2 ORDER BY 1, 2"
docker compose exec sources-db psql -U sources -d sources -c "SELECT customer, status, count(*) FROM wms_sud.outbound GROUP BY 1, 2 ORDER BY 1, 2"
```

Les fichiers sont déposés, un par jour et par type :

```bash
ls depot/transporteurs | wc -l
ls depot/kelio | wc -l
ls depot/edi | wc -l
```
Attendu : 120 (60 GEODIS + 60 DPD), 60, 60.

Les événements sont dans Kafka :

```bash
docker compose exec redpanda rpk topic describe erp.commandes -p
```
Attendu : la somme des `HIGH-WATERMARK` des 3 partitions est égale au nombre d'« événements ERP » affiché par le
générateur.

### Voir
- **pgAdmin**, connexion 3 → *wms_nord → commandes* et *wms_sud → outbound* : mêmes informations, colonnes,
  codes et statuts différents.
- **Redpanda Console** → *Topics → erp.commandes → Messages* : les événements JSON de l'ERP de CLI004, avec leur
  partition et leur offset.
- **Finder** → `depot/transporteurs` : comparer un fichier `geodis_*.json` et un fichier `dpd_*.csv`.

---

## 8. Étape 3 — Charger la zone brute

```bash
docker compose run --rm outils python chargeur/charger.py
```

Résultat attendu : le détail par table, puis `Total chargé dans raw : ~94 800 lignes`.

### Ce que fait `chargeur/charger.py`

Il copie tout dans le schéma `raw`, **sans rien transformer**, en trois temps. Chaque mécanisme garantit qu'une
relance ne charge jamais deux fois la même chose.

| Source | Mécanisme | Où ça arrive | Protection contre les doublons |
|---|---|---|---|
| Bases (9 tables) | lit les lignes dont la date de modification dépasse le **filigrane**, la dernière date déjà extraite pour cette table | `raw.extraction` : une ligne par version, la ligne source complète en JSON dans `payload` | le filigrane est mis à jour dans la même transaction (`raw.filigrane`) |
| Fichiers de `depot/` | lit chaque fichier, insère chaque ligne, puis déplace le fichier dans `depot/archive/` | `raw.fichiers` (une ligne par ligne de fichier) | registre `raw.fichiers_charges` : un nom de fichier n'est chargé qu'une fois |
| Kafka `erp.commandes` | consomme les messages avec le groupe `chargeur-dwh` | `raw.evenements` (topic, partition, offset, contenu) | l'offset n'est validé **qu'après** l'écriture en base, et la contrainte unique (topic, partition, offset) ignore un message rejoué |

Le chargeur se connecte aux sources en **lecture seule** (rôle `extraction`) et à l'entrepôt avec un rôle qui ne
peut écrire que dans `raw` (rôle `chargeur`).

### Vérifier

Chaque table source est entièrement arrivée : comparez avec les comptes de l'étape 2.

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT source, table_source, count(*) AS lignes, max(maj_source) AS derniere_modif
FROM raw.extraction GROUP BY 1, 2 ORDER BY 1, 2"
```
Attendu : exactement les mêmes nombres que dans les sources (une seule version par ligne à ce stade).

Les filigranes indiquent jusqu'où chaque table a été lue :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "SELECT * FROM raw.filigrane ORDER BY 1, 2"
```

Les fichiers sont chargés, puis archivés :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT type_fichier, count(*) AS fichiers, sum(nb_lignes) AS lignes
FROM raw.fichiers_charges GROUP BY 1 ORDER BY 1"
ls depot/transporteurs | wc -l
ls depot/archive/transporteurs | wc -l
```
Attendu : 60 fichiers par type, `depot/transporteurs` vide (0) et 120 fichiers dans `depot/archive/transporteurs`.

Tous les messages Kafka sont arrivés, sans retard du consommateur :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT partition, count(*) AS messages, max(position) + 1 AS jusqu_a_offset
FROM raw.evenements GROUP BY 1 ORDER BY 1"
docker compose exec redpanda rpk group describe chargeur-dwh
```
Attendu : pour chaque partition, le nombre de messages égale l'offset atteint et le `HIGH-WATERMARK` de l'étape 2 ;
`TOTAL-LAG 0`.

Une ligne brute, telle qu'elle a été copiée :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT cle, maj_source, payload FROM raw.extraction WHERE source = 'wms_sud' AND table_source = 'outbound' LIMIT 1"
```

Relancez le chargeur : `Total chargé dans raw : 0 lignes`. L'extraction est incrémentale.

### Voir
- **pgAdmin**, connexion 1 → *raw → extraction* : la colonne `payload` contient la ligne source en JSON.
- **Redpanda Console** → *Consumer Groups → chargeur-dwh* : la position du chargeur sur chaque partition, et son
  retard (*lag*) à 0.

---

## 9. Étape 4 — Transformer et tester (dbt)

```bash
docker compose run --rm outils dbt build --project-dir dbt --profiles-dir dbt
```

Résultat attendu : `Found 30 models, 26 data tests, 4 operations…`, puis
`Done. PASS=60 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=60`.

### Ce que fait `dbt build`

Il construit les couches `staging` → `core` → `marts` → `api` dans l'ordre des dépendances, teste chaque modèle
juste après l'avoir construit, puis donne les droits de lecture (section 4, « dbt en bref »).

| Modèle | Ce qu'il fait |
|---|---|
| `staging.stg_commandes` | réunit les deux WMS : `C-004` et `CLI004` deviennent un seul client, `60` et `EXPEDIEE` un seul statut (via `ref`) |
| `staging.stg_suivi_transporteurs` | aligne GEODIS et DPD sur les mêmes statuts, convertit la date DPD `JJ/MM/AAAA` |
| `core.fct_commandes` | une ligne par commande : délai d'expédition, expédiée le jour même |
| `core.fct_expeditions` | une ligne par expédition avec prise en charge, livraison, incident, délai |
| `marts.mart_logistique_jour` | volumes par jour, site et client ; taux d'expédition le jour même |
| `marts.mart_facturation_mois` | volumes × tarif valable le jour de la prestation, par mois |
| `marts.mart_rh_temps` | heures et effectifs par site, jour, activité |
| `marts.mart_productivite` | unités par heure (palettes, lignes, colis) et écart à l'objectif |
| `marts.mart_qualite_anomalies` | rapprochements : EDI sans commande WMS, commande CLI004 sans événement ERP, expédition sans commande, client non référencé, plus de 12 h dans Kelio |

### Vérifier

Les objets créés par dbt :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT table_schema AS schema, table_type, count(*) AS objets
FROM information_schema.tables WHERE table_schema IN ('staging', 'core', 'marts', 'api')
GROUP BY 1, 2 ORDER BY 1"
```
Attendu : `api` 3 vues, `core` 9 tables, `marts` 8 tables, `staging` 10 vues.

L'harmonisation : un même client vu par les deux WMS, avec leurs codes et statuts d'origine, puis harmonisés.

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT site, code_source, code_client, statut_source, statut, count(*)
FROM staging.stg_commandes WHERE code_client = 'CLI003' GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 4"
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT transporteur, code_evenement, statut, count(*)
FROM staging.stg_suivi_transporteurs GROUP BY 1, 2, 3 ORDER BY 1, 3"
```
Attendu : à Lille, `CLI003` / `EXPEDIEE` ; à Lyon, `C-003` / `60`, et les deux harmonisés en `CLI003` / `EXPEDIEE`.
Côté transporteurs, `DELIVERED` (DPD) et `LIVRE` (GEODIS) deviennent tous deux `LIVRE`.

Le rapprochement entre la zone brute et `core` : rien n'est perdu, tout est harmonisé.

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT (SELECT count(DISTINCT cle) FROM raw.extraction WHERE table_source IN ('commandes', 'outbound')) AS commandes_raw,
       (SELECT count(*) FROM core.fct_commandes) AS commandes_core,
       (SELECT count(*) FROM core.fct_commandes WHERE code_client IS NULL OR statut IS NULL) AS non_harmonisees"
```
Attendu : `commandes_raw` = `commandes_core` (~15 100), et `non_harmonisees` = 0.

Les contrôles qualité :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT controle, gravite, count(*) FROM marts.mart_qualite_anomalies GROUP BY 1, 2"
```
Attendu : une seule ligne, `COMMANDE_WMS_SANS_EVENEMENT_ERP` / `MOYENNE`, avec un compte d'environ 1 % des
commandes de CLI004 à Lyon (≈ 4 300 commandes), soit **40 à 60** selon les dates générées. Ce sont les événements
ERP volontairement perdus. Les anomalies de gravité `ELEVEE` font échouer le build
(`tests/assert_aucune_anomalie_elevee.sql`) : il ne doit jamais y en avoir ici.

Un reporting prêt à l'emploi :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT jour, site, code_client, nb_commandes_creees, nb_commandes_expediees, taux_expedition_j0_pct, cout_transport_ht
FROM marts.mart_logistique_jour WHERE jour = current_date - 1 ORDER BY site, code_client"
```

### Voir : documentation et lignage

```bash
docker compose run --rm outils dbt docs generate --project-dir dbt --profiles-dir dbt
```

Puis ouvrez http://localhost:8083 (le service `dbt-docs` tourne en permanence et sert la dernière documentation
générée) :
- l'arbre de gauche liste les modèles par couche ; cliquez sur un modèle pour voir son SQL, ses colonnes et ses tests ;
- le bouton bleu en bas à droite affiche le **graphe de lignage** : `raw` → `staging` → `core` → `marts` → `api`.

Relancez `dbt docs generate` après chaque modification des modèles.

Dans **pgAdmin**, connexion 1 : les schémas `staging`, `core`, `marts` et `api` sont apparus (clic droit sur
*Schemas* → *Refresh*).

**Metabase** a été connecté avant que dbt crée `core` et `marts` : demandez-lui de relire les tables.

```bash
docker compose run --rm config-interfaces
```
La commande ne refait rien de ce qui existe déjà ; elle lance seulement la synchronisation. Comptez une quinzaine de
secondes avant que les tables apparaissent dans Metabase. À refaire après chaque `dbt build` qui crée de nouvelles
tables.

---

## 10. Étape 5 — Reportings (Metabase, Power BI, Qlik)

**Metabase** joue ici le rôle de Qlik ou Power BI : c'est ce que voit un contrôleur de gestion ou un responsable de
site.

1. Ouvrez http://localhost:3000 et connectez-vous avec `admin@example.com` / `Admin-Kit-2026`.
2. La base **« Entrepôt logistique (dwh) »** est déjà connectée, avec le rôle `lecture_bi`. Si les schémas `core` et
   `marts` n'apparaissent pas, lancez `docker compose run --rm config-interfaces` (fin de l'étape 4).
3. *Parcourir les données* → *Entrepôt logistique (dwh)* → *Marts* : `Mart Logistique Jour`, `Mart Facturation
   Mois`, `Mart Rh Temps`, `Mart Productivite`.
4. Pour un graphique : *+ Nouveau* → *Question* → table `Mart Logistique Jour`, résumer *Somme de Nb Commandes
   Creees* par *Jour* et *Site*, puis *Visualiser*.

Paramètres de connexion, si vous devez l'ajouter à la main :

| Champ | Valeur |
|---|---|
| Type | PostgreSQL |
| Hôte | `dwh` |
| Port | `5432` |
| Base | `dwh` |
| Utilisateur / mot de passe | `lecture_bi` / `lecture_bi` |

**Power BI Desktop ou Qlik Sense**, sur le même poste :
- connecteur PostgreSQL ;
- serveur `localhost:55432`, base `dwh` ;
- utilisateur `lecture_bi` / `lecture_bi`.

### Vérifier : ce que voit l'outil BI

```bash
docker compose exec dwh psql -U lecture_bi -d dwh -c "SELECT count(*) FROM marts.mart_logistique_jour"
docker compose exec dwh psql -U lecture_bi -d dwh -c "SELECT count(*) FROM raw.extraction"
```
Attendu : la première renvoie un nombre ; la seconde est refusée (`permission denied for schema raw`). Les outils
BI ne voient que les données contrôlées.

**Voir** : dans **pgAdmin**, la connexion 2 (`lecture_bi`) montre le même refus quand on ouvre une table de `raw`.

---

## 11. Étape 6 — Portail des référentiels (NocoDB)

**À quoi sert NocoDB.** C'est le « portail de référentiels externes » du schéma. Les tarifs, les correspondances de
codes clients et les traductions de statuts ne viennent d'aucun système source : ils sont tenus par le métier
(contrôle de gestion, ADV). NocoDB leur donne un **tableur web branché directement sur le schéma `ref`**, sans SQL.
Une modification y est prise en compte au `dbt build` suivant, sans toucher au code.

Vous n'en avez pas besoin pour le reste du parcours : chaque manipulation a son équivalent SQL ci-dessous.

1. Ouvrez http://localhost:8081 et connectez-vous (**SIGN IN**) avec `admin@example.com` / `Admin-Kit-2026`.
2. Ouvrez la base **« Référentiels (entrepôt dwh) »**, déjà branchée sur le schéma `ref` avec le rôle `portail_ref` :
   `clients`, `correspondance_clients`, `objectifs_productivite`, `sites`, `statuts`, `tarifs`.
3. Ouvrez la table `tarifs` et passez le `prix_unitaire` de `CLI001` / `PREPARATION_LIGNE` de 0,42 à 0,45.
4. Relancez dbt et comparez la facturation :

```bash
docker compose run --rm outils dbt build --project-dir dbt --profiles-dir dbt --select +mart_facturation_mois
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT mois, prestation, quantite, montant_ht FROM marts.mart_facturation_mois WHERE code_client='CLI001' ORDER BY 1, 2"
```

Le `--select +mart_facturation_mois` ne reconstruit que ce mart et ses dépendances : `PASS=31` au lieu de 60. Seules
les lignes `PREPARATION_LIGNE` changent (+7 % environ).

Même modification sans NocoDB :

```bash
docker compose exec dwh psql -U portail_ref -d dwh -c \
 "UPDATE ref.tarifs SET prix_unitaire = 0.45 WHERE code_client='CLI001' AND prestation='PREPARATION_LIGNE'"
```

### Vérifier

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT prestation, sum(montant_ht) FROM marts.mart_facturation_mois WHERE code_client = 'CLI001' GROUP BY 1 ORDER BY 1"
```
Lancez cette requête avant et après la modification : seul `PREPARATION_LIGNE` change. Pour revenir au tarif
d'origine, refaites l'`UPDATE` avec `0.42`, puis relancez le `dbt build --select`.

---

## 12. Étape 7 — IA : prévision des commandes à 7 jours

```bash
docker compose run --rm outils python services/prevoir_volumes.py
```

### Ce que fait `services/prevoir_volumes.py`

Pour chaque couple site × client, il lit l'historique dans `marts.mart_logistique_jour`, puis :
- entraîne un gradient boosting sur le jour de la semaine et les volumes à J-7, J-14 et la moyenne sur 7 jours ;
- le valide sur les 14 derniers jours ;
- le compare à la référence « même jour la semaine précédente » (J-7). L'écart moyen est la MAE : plus elle est
  basse, meilleure est la prévision ;
- écrit les 7 prochains jours dans `ia.previsions_commandes`, ce qui fait 42 lignes (6 couples × 7 jours).

Le modèle ne bat pas toujours cette référence : c'est le résultat attendu à observer, pas un défaut du kit.

### Vérifier

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT jour, to_char(jour, 'TMDay') AS jour_semaine, site, code_client, commandes_prevues, mae_modele, mae_reference
FROM ia.previsions_commandes WHERE code_client = 'CLI004' ORDER BY jour"
```
Attendu : 7 lignes. Le samedi est nettement plus bas et le dimanche proche de 0 : le modèle a appris le rythme de la
semaine.

**Voir** : dans Metabase ou **pgAdmin** (connexion 2), la table `ia.previsions_commandes`.

---

## 13. Étape 8 — Services clients

### Exports CSV

```bash
docker compose run --rm outils python services/exporter_csv.py
ls exports/CLI003
head -3 exports/CLI003/expeditions_*.csv
```

**Ce que fait le script.** Avec le rôle `lecture_bi`, il lit `marts.mart_client_expeditions` (commandes créées ou
expédiées hier) et `marts.mart_client_stock` (dernière photo), puis écrit deux fichiers CSV (séparateur `;`) par
client actif dans `exports/<client>/`. Pour un autre jour : `--jour AAAA-MM-JJ`.

**Vérifier** : un dossier par client (`CLI001` à `CLI005`), chacun avec `expeditions_<date>.csv` et `stock_<date>.csv`.

### Portail client (API)

Chaque client ne voit que ses lignes.

1. Relancez PostgREST pour qu'il découvre le schéma `api` créé par dbt :
   ```bash
   docker compose restart postgrest
   ```
2. Créez un jeton pour CLI003 et interrogez l'API :
   ```bash
   TOKEN=$(docker compose run --rm -T outils python services/jeton_client.py CLI003 2>/dev/null)
   curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:3001/v_expeditions?statut_transport=eq.INCIDENT&limit=3"
   curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:3001/v_stock?site=eq.LYO&limit=3"
   ```
3. Vérifiez qu'un appel sans jeton est refusé :
   ```bash
   curl -s "http://localhost:3001/v_expeditions?limit=1"
   ```

**Comment ça marche.** `jeton_client.py` fabrique un jeton JWT signé, valable 8 h, qui contient
`{"role": "portail_client", "client": "CLI003"}`. PostgREST vérifie la signature, prend le rôle PostgreSQL
`portail_client` et rend le contenu du jeton lisible en SQL. Les vues `api.*` filtrent sur
`current_setting('request.jwt.claims')::json->>'client'`. Le rôle `portail_client` n'a aucun droit sur `marts` : il
ne peut pas contourner le filtre.

**Vérifier** que le jeton ne donne accès qu'à CLI003 :

```bash
echo "$TOKEN" | cut -d. -f2 | base64 -d 2>/dev/null; echo
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:3001/v_expeditions?select=code_client" | grep -o '"code_client":"[^"]*"' | sort | uniq -c
```
Attendu : le contenu du jeton, puis un seul code client, `CLI003`, avec son nombre de lignes. Sans jeton :
`permission denied for view v_expeditions`.

### Événements auxquels s'abonner

Un topic par client.

```bash
docker compose run --rm outils python services/publier_evenements.py
docker compose exec redpanda rpk topic consume clients.CLI003.expeditions -n 3
```

**Ce que fait le script.** Il lit dans `marts.mart_client_expeditions` les commandes modifiées depuis le dernier
filigrane (`diffusion.filigrane`), publie un message `STATUT_COMMANDE` par commande dans le topic de son client, puis
avance le filigrane. Relancez la publication : `Aucun changement depuis la dernière publication.`

**Vérifier** :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "SELECT * FROM diffusion.filigrane"
docker compose exec redpanda rpk topic describe clients.CLI003.expeditions -p
```
Attendu : le filigrane à la date du dernier changement publié, et un `HIGH-WATERMARK` égal au nombre d'événements
publiés pour CLI003.

**Voir** : **Redpanda Console** → *Topics → clients.CLI003.expeditions → Messages*.

---

## 14. Étape 9 — Simuler un jour de plus (chaîne complète)

```bash
docker compose run --rm outils python generateur/generer.py --jour-suivant
docker compose run --rm outils python chargeur/charger.py
docker compose run --rm outils dbt build --project-dir dbt --profiles-dir dbt
docker compose run --rm outils python services/publier_evenements.py
```

**Ce qui se passe.** Le générateur ajoute un jour et passe à « expédiée » les commandes restées « préparées » la
veille. Le chargeur ne ramène que ce qui a changé depuis les filigranes (~2 000 lignes au lieu de ~95 000). Dans
`raw.extraction`, les commandes modifiées ont désormais **deux versions** ; dbt ne retient que la plus récente.

### Vérifier

L'historique dans la zone brute :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT cle, maj_source, payload->>'statut' AS statut FROM raw.extraction
  WHERE table_source='commandes' AND cle IN (SELECT cle FROM raw.extraction WHERE table_source='commandes'
  GROUP BY cle HAVING count(*) > 1 LIMIT 2) ORDER BY cle, maj_source"
```
Attendu : deux lignes par commande, `PREPAREE` puis `EXPEDIEE`.

`core` ne garde que la dernière version :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT count(*) AS lignes_raw, count(DISTINCT (source, table_source, cle)) AS objets_distincts,
       count(*) - count(DISTINCT (source, table_source, cle)) AS versions_historisees
FROM raw.extraction"
```
Attendu : environ 100 versions historisées (les commandes expédiées le lendemain, plus quelques tickets résolus).

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT max(jour) FROM marts.mart_logistique_jour WHERE nb_commandes_creees > 0"
```
Attendu : la date du jour ajouté.

---

## 15. Étape 10 — La qualité bloque une livraison

Un WMS envoie un code client inconnu :

```bash
docker compose exec sources-db psql -U sources -d sources -c \
 "INSERT INTO wms_sud.outbound (ref, customer, created_at, status, lines, parcels, updated_at)
  SELECT 'CMD-LYO-TEST-C-009-0001', 'C-009', now(), 10, 3, 1, max(updated_at) + interval '1 minute'
  FROM wms_sud.outbound"
docker compose run --rm outils python chargeur/charger.py
docker compose run --rm outils dbt build --project-dir dbt --profiles-dir dbt
```

Attendu : `FAIL 1 not_null_fct_commandes_code_client`, puis `Done. PASS=49 WARN=0 ERROR=1 SKIP=10`. Les dix
éléments en aval (marts, vues API et leurs tests) sont `SKIP` : ils ne sont pas reconstruits, et les tableaux de
bord gardent les chiffres de la veille, qui eux sont justes.

### Diagnostiquer

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT id_commande, code_client, statut FROM core.fct_commandes WHERE code_client IS NULL"
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT site, code_source, statut_source FROM staging.stg_commandes WHERE code_client IS NULL"
```
Attendu : la commande fautive, avec un `code_client` vide dans `core`. Dans `staging`, son code d'origine `C-009`,
absent de `ref.correspondance_clients`.

### Corriger

Dans NocoDB : ajoutez la ligne `CLI006` / `Nouveau client` / `true` dans `clients`, puis `wms_sud` / `C-009` /
`CLI006` dans `correspondance_clients`. Ou en SQL, ci-dessous (avec les tarifs de CLI006). Puis relancez dbt.
Attendu : `PASS=60 … ERROR=0`.

```bash
docker compose exec dwh psql -U portail_ref -d dwh -c \
 "INSERT INTO ref.clients VALUES ('CLI006', 'Nouveau client', true);
  INSERT INTO ref.correspondance_clients VALUES ('wms_sud', 'C-009', 'CLI006');
  INSERT INTO ref.tarifs (code_client, prestation, prix_unitaire, valide_du)
  SELECT 'CLI006', prestation, prix_unitaire, valide_du FROM ref.tarifs WHERE code_client = 'CLI001'"
docker compose run --rm outils dbt build --project-dir dbt --profiles-dir dbt
```

La commande est désormais rattachée à `CLI006` :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c \
 "SELECT id_commande, code_client, statut FROM core.fct_commandes WHERE num_commande = 'CMD-LYO-TEST-C-009-0001'"
```

---

## 16. Bilan de toute la chaîne en une commande

À lancer à tout moment pour vérifier l'état de l'entrepôt, couche par couche :

```bash
docker compose exec dwh psql -U dwh_admin -d dwh -c "
SELECT '1 raw' AS couche, 'lignes extraites des bases (toutes versions)' AS controle, count(*)::text AS valeur FROM raw.extraction
UNION ALL SELECT '1 raw', 'lignes de fichiers chargées', count(*)::text FROM raw.fichiers
UNION ALL SELECT '1 raw', 'messages Kafka reçus', count(*)::text FROM raw.evenements
UNION ALL SELECT '2 staging', 'commandes harmonisées (2 WMS)', count(*)::text FROM staging.stg_commandes
UNION ALL SELECT '3 core', 'commandes sans client reconnu (doit être 0)', count(*)::text FROM core.fct_commandes WHERE code_client IS NULL
UNION ALL SELECT '3 core', 'expéditions', count(*)::text FROM core.fct_expeditions
UNION ALL SELECT '4 marts', 'dernier jour avec activité', max(jour)::text FROM marts.mart_logistique_jour WHERE nb_commandes_creees > 0
UNION ALL SELECT '4 marts', 'anomalies qualité ÉLEVÉES (doit être 0)', count(*)::text FROM marts.mart_qualite_anomalies WHERE gravite = 'ELEVEE'
UNION ALL SELECT '4 marts', 'anomalies qualité MOYENNES', count(*)::text FROM marts.mart_qualite_anomalies WHERE gravite = 'MOYENNE'
UNION ALL SELECT '5 ia', 'prévisions à 7 jours', count(*)::text FROM ia.previsions_commandes
UNION ALL SELECT '6 diffusion', 'événements publiés jusqu''à', max(dernier_maj)::text FROM diffusion.filigrane
ORDER BY 1, 2"
```

Après les étapes 1 à 9 : environ 39 300 / 53 200 / 4 350 lignes dans `raw`, ~15 400 commandes, 0 commande sans
client, environ 15 000 expéditions, le dernier jour généré, 0 anomalie élevée, 40 à 60 anomalies moyennes et 42
prévisions. La requête suppose que dbt a déjà tourné une fois (étape 4).

---

## 17. Arrêter, réinitialiser

| Besoin | Commande |
|---|---|
| Arrêter sans rien perdre | `docker compose stop` |
| Reprendre | `docker compose start` |
| Tout remettre à zéro (bases, topics, Metabase, NocoDB, pgAdmin, fichiers générés) | voir ci-dessous |
| Supprimer aussi l'image outils | `docker compose --profile outils down -v --rmi local` |

**Tout remettre à zéro**, depuis le dossier du kit :

```bash
docker compose --profile outils down -v
rm -rf depot/transporteurs depot/kelio depot/edi depot/archive dbt/target dbt/logs
find exports -mindepth 1 -maxdepth 1 -name 'CLI*' -exec rm -rf {} +
```

- `--profile outils` supprime aussi les conteneurs `outils` éventuellement restés.
- Les fichiers de `depot/` doivent être supprimés : sinon, le chargeur les relirait après la remise à zéro.
- `dbt/target` contient la documentation générée : on la supprime pour que http://localhost:8083 ne montre pas
  l'ancienne.
- `find` plutôt que `rm exports/CLI*` : sous zsh, un motif qui ne trouve rien (aucun export encore produit) fait
  échouer **toute** la commande, et rien n'est supprimé.

Après une remise à zéro, reprenez à l'étape 1.

---

## 18. Dépannage

| Symptôme | Cause | Solution |
|---|---|---|
| `port is already allocated` | port déjà utilisé sur le poste | changer le port de gauche dans `docker-compose.yml` (ex. `"55442:5432"`) |
| `Des données existent déjà` au générateur | `--jours` déjà lancé | utiliser `--jour-suivant`, ou réinitialiser |
| Chargeur bloqué sur Kafka | Redpanda pas encore prêt | `docker compose ps redpanda` jusqu'à `healthy` |
| Pas de Redpanda (poste limité) | — | `generer.py --sans-kafka` et `charger.py --sans-kafka` ; `publier_evenements.py --afficher` |
| PostgREST : `relation api.v_expeditions does not exist` ou schéma introuvable | démarré avant le premier `dbt build` | `docker compose restart postgrest` |
| PostgREST : `JWT expired` | jeton de plus de 8 h | regénérer le jeton |
| dbt : `permission denied` | commande lancée hors du conteneur outils | toujours passer par `docker compose run --rm outils …` |
| http://localhost:8083 : « Documentation dbt pas encore générée » | `dbt docs generate` pas encore lancé | la commande de l'étape 4, puis recharger la page |
| pgAdmin ne montre pas `staging`, `core`, `marts` | arbre chargé avant le premier `dbt build` | clic droit sur *Schemas* → *Refresh* |
| pgAdmin, connexion 2 : `permission denied for schema raw` | comportement voulu : `lecture_bi` ne lit pas `raw` | utiliser la connexion 1 |
| Metabase ne montre que `ref` | connecté avant le premier `dbt build` | `docker compose run --rm config-interfaces`, puis attendre 15 s |
| `config-interfaces` en `Exited (1)` | Metabase ou NocoDB pas prêts à temps (poste lent, mémoire) | `docker compose logs config-interfaces`, puis `docker compose run --rm config-interfaces` |
| NocoDB : « Connection to internal hosts is not allowed » en ajoutant une base à la main | protection de NocoDB contre les bases du réseau interne | déjà levée dans ce kit par `NC_ALLOW_LOCAL_EXTERNAL_DBS: "true"` ; utiliser la base préconfigurée |
| Écran **SIGN UP** de NocoDB au lieu de **SIGN IN** | volume NocoDB créé par une version précédente du kit, sans compte | `docker compose rm -sf nocodb`, `docker volume rm dwh-logistique-v1_0_nocodb-data`, puis `docker compose up -d` |
| Conteneur arrêté en `Exited (137)` | mémoire Docker insuffisante | 6 Go minimum ; arrêter Metabase si besoin (`docker compose stop metabase`) |
| `zsh: no matches found` | URL ou motif de fichier sans guillemets : zsh interprète `?`, `*`, `&` | mettre l'URL entre guillemets ; pour les fichiers, utiliser les commandes de la section 17 |
| Données inattendues dès le premier lancement (`Des données existent déjà`) | anciennes données d'un autre kit portant le même nom de projet | ce kit s'appelle `dwh-logistique-v1_0` ; supprimer les volumes de l'ancien projet : `docker volume ls` puis `docker volume rm …` |

---

## 19. Vers la production

| Élément du kit | En production |
|---|---|
| `sources-db` simulée | lecture sur un réplica en lecture seule de chaque WMS / TMS, ou CDC (Debezium) pour ne rien manquer entre deux extractions |
| dossier `depot/` | SFTP des transporteurs et des clients EDI, ou stockage objet (MinIO, S3) |
| commandes lancées à la main | orchestrateur (Airflow, Dagster) : générer → charger → dbt → exports → événements, avec alertes |
| mots de passe en clair | coffre de secrets (Vault, Azure Key Vault…), comptes nominatifs |
| PostgREST | derrière une passerelle d'API (authentification des clients, quotas), jetons émis par l'annuaire |
| Redpanda un nœud | cluster Kafka ou Redpanda à 3 nœuds, ACL par topic client |
| PostgreSQL un conteneur | serveur dédié ou service géré, sauvegardes pgBackRest et réplica ; ClickHouse si les volumes l'exigent |
| pgAdmin sans mot de passe, comptes Metabase / NocoDB fixes | mode serveur avec authentification, comptes nominatifs reliés à l'annuaire (SSO) |

---

## 20. Fichiers du kit

```
kit-dwh-logistique-v1_0/
├── README.md
├── docker-compose.yml
├── sources/01_schema_sources.sql        bases sources simulées (WMS nord / sud, TMS, ticketing, DONUT)
├── dwh/01_init_dwh.sql                  zones raw, ref, ia, diffusion ; référentiels ; rôles
├── pgadmin/
│   ├── servers.json                     connexions préconfigurées de pgAdmin
│   └── pgpass                           mots de passe de ces connexions (démonstration)
├── outils/
│   ├── Dockerfile, requirements.txt
│   ├── generateur/generer.py            activité simulée (bases, fichiers, Kafka)
│   ├── chargeur/charger.py              extraction incrémentale → raw
│   └── services/
│       ├── exporter_csv.py              exports clients
│       ├── publier_evenements.py        événements par client (Kafka)
│       ├── prevoir_volumes.py           prévision IA
│       ├── jeton_client.py              jeton JWT de test pour le portail
│       ├── servir_docs.py               serveur de la documentation dbt (service dbt-docs)
│       └── configurer_interfaces.py     prépare Metabase et NocoDB (service config-interfaces)
├── dbt/
│   ├── dbt_project.yml, profiles.yml
│   ├── macros/                          nommage des schémas, dernière version, tests maison
│   ├── models/staging/                  harmonisation des sources
│   ├── models/core/                     faits et dimensions (+ tests)
│   ├── models/marts/                    reportings, facturation, productivité, qualité, suivi client
│   ├── models/api/                      vues filtrées par client pour PostgREST
│   └── tests/                           contrôles bloquants et avertissements
├── depot/                               fichiers déposés par les partenaires (généré)
└── exports/                             exports CSV clients (généré)
```

| Élément | Valeur |
|---|---|
| Entrepôt | `localhost:55432`, base `dwh` — `dwh_admin` / `dwh_admin` (administration), `lecture_bi` / `lecture_bi` (BI) |
| Metabase et NocoDB | `admin@example.com` / `Admin-Kit-2026` (créés automatiquement au démarrage) |
| Sources | `localhost:55433`, base `sources` — `sources` / `sources` |
| Kafka depuis le poste | `localhost:19092` |
| Versions | PostgreSQL 17 · Redpanda 24.2.7 · dbt-core 1.11.15 / dbt-postgres 1.11.0 · Python 3.12 · PostgREST 12.2.3 · Metabase 0.52.4 · pgAdmin 9.18 |
| Projet Compose | `dwh-logistique-v1_0` (conteneurs `dwh-logistique-v1_0-…`) |
