-- Entrepôt de données : zone brute (raw), référentiels (ref), diffusion,
-- rôles d'accès. Les schémas staging, core, marts et api sont créés par dbt.

CREATE SCHEMA raw;
CREATE SCHEMA ref;
CREATE SCHEMA ia;
CREATE SCHEMA diffusion;

-- ---------- Zone brute : tout ce qui arrive, jamais modifié ----------
CREATE TABLE raw.extraction (                 -- lignes extraites des bases sources
  id           bigserial PRIMARY KEY,
  source       text        NOT NULL,          -- wms_nord, wms_sud, tms, ticketing, donut
  table_source text        NOT NULL,
  cle          text        NOT NULL,          -- clé primaire dans la source
  maj_source   timestamp   NOT NULL,          -- date de modification dans la source
  payload      jsonb       NOT NULL,          -- la ligne complète
  extrait_le   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON raw.extraction (source, table_source, cle, maj_source DESC);

CREATE TABLE raw.filigrane (                  -- dernière date de modification extraite
  source       text NOT NULL,
  table_source text NOT NULL,
  dernier_maj  timestamp NOT NULL,
  PRIMARY KEY (source, table_source)
);

CREATE TABLE raw.fichiers (                   -- lignes des fichiers déposés (EDI, transporteurs, Kelio)
  id          bigserial PRIMARY KEY,
  type_fichier text       NOT NULL,           -- transporteur_geodis, transporteur_dpd, kelio, edi_orders
  nom_fichier text        NOT NULL,
  num_ligne   int         NOT NULL,
  payload     jsonb       NOT NULL,
  charge_le   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE raw.fichiers_charges (           -- un fichier n'est jamais chargé deux fois
  nom_fichier text PRIMARY KEY,
  type_fichier text NOT NULL,
  nb_lignes   int  NOT NULL,
  charge_le   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE raw.evenements (                 -- messages Kafka reçus des ERP clients
  id          bigserial PRIMARY KEY,
  topic       text   NOT NULL,
  partition   int    NOT NULL,
  position    bigint NOT NULL,                -- offset Kafka
  cle         text,
  payload     jsonb  NOT NULL,
  recu_le     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (topic, partition, position)
);

-- ---------- Référentiels, modifiables par le portail (NocoDB) ----------
CREATE TABLE ref.clients (
  code_client text PRIMARY KEY,
  nom         text NOT NULL,
  actif       boolean NOT NULL DEFAULT true
);
CREATE TABLE ref.sites (
  code_site text PRIMARY KEY,
  nom       text NOT NULL,
  wms       text NOT NULL
);
CREATE TABLE ref.correspondance_clients (     -- code du client dans chaque source → code unique
  source      text NOT NULL,
  code_source text NOT NULL,
  code_client text NOT NULL REFERENCES ref.clients,
  PRIMARY KEY (source, code_source)
);
CREATE TABLE ref.statuts (                    -- statut de chaque source → statut harmonisé
  source      text NOT NULL,
  code_source text NOT NULL,
  statut      text NOT NULL,
  PRIMARY KEY (source, code_source)
);
CREATE TABLE ref.tarifs (
  code_client   text NOT NULL REFERENCES ref.clients,
  prestation    text NOT NULL,                -- RECEPTION_PALETTE, PREPARATION_LIGNE, EXPEDITION_COLIS, ETIQUETAGE, REEMBALLAGE
  prix_unitaire numeric(10,4) NOT NULL,
  valide_du     date NOT NULL,
  valide_au     date,                         -- vide = toujours valable
  PRIMARY KEY (code_client, prestation, valide_du)
);
CREATE TABLE ref.objectifs_productivite (
  code_site       text NOT NULL REFERENCES ref.sites,
  activite        text NOT NULL,              -- RECEPTION (palettes/h), PREPARATION (lignes/h), EXPEDITION (colis/h)
  unites_par_heure numeric(8,2) NOT NULL,
  PRIMARY KEY (code_site, activite)
);

INSERT INTO ref.clients VALUES
 ('CLI001','Maison Lefèvre', true), ('CLI002','Sportissimo', true), ('CLI003','BioMarché', true),
 ('CLI004','TechnoPlus', true), ('CLI005','Jouets & Co', true);
INSERT INTO ref.sites VALUES ('LIL','Lesquin (59)','wms_nord'), ('LYO','Saint-Priest (69)','wms_sud');
INSERT INTO ref.correspondance_clients VALUES
 ('wms_nord','CLI001','CLI001'), ('wms_nord','CLI002','CLI002'), ('wms_nord','CLI003','CLI003'),
 ('wms_sud','C-003','CLI003'), ('wms_sud','C-004','CLI004'), ('wms_sud','C-005','CLI005');
INSERT INTO ref.statuts VALUES
 ('wms_nord','ATTENDUE','ATTENDUE'), ('wms_nord','EN_COURS','EN_COURS'), ('wms_nord','CLOTUREE','CLOTUREE'),
 ('wms_nord','CREEE','CREEE'), ('wms_nord','PREPAREE','PREPAREE'), ('wms_nord','EXPEDIEE','EXPEDIEE'), ('wms_nord','ANNULEE','ANNULEE'),
 ('wms_sud_inbound','10','ATTENDUE'), ('wms_sud_inbound','20','EN_COURS'), ('wms_sud_inbound','30','CLOTUREE'),
 ('wms_sud_outbound','10','CREEE'), ('wms_sud_outbound','40','PREPAREE'), ('wms_sud_outbound','60','EXPEDIEE'), ('wms_sud_outbound','90','ANNULEE'),
 ('geodis','PRIS_EN_CHARGE','PRIS_EN_CHARGE'), ('geodis','EN_TRANSIT','EN_TRANSIT'), ('geodis','LIVRE','LIVRE'), ('geodis','INCIDENT','INCIDENT'),
 ('dpd','PICKUP','PRIS_EN_CHARGE'), ('dpd','TRANSIT','EN_TRANSIT'), ('dpd','DELIVERED','LIVRE'), ('dpd','EXCEPTION','INCIDENT');
INSERT INTO ref.tarifs (code_client, prestation, prix_unitaire, valide_du)
SELECT c.code_client, p.prestation, p.prix, DATE '2025-01-01'
FROM ref.clients c
CROSS JOIN (VALUES ('RECEPTION_PALETTE', 4.80), ('PREPARATION_LIGNE', 0.42), ('EXPEDITION_COLIS', 1.15),
                   ('ETIQUETAGE', 0.18), ('REEMBALLAGE', 0.95)) AS p(prestation, prix);
INSERT INTO ref.objectifs_productivite VALUES
 ('LIL','RECEPTION',12), ('LIL','PREPARATION',45), ('LIL','EXPEDITION',60),
 ('LYO','RECEPTION',11), ('LYO','PREPARATION',42), ('LYO','EXPEDITION',55);

-- ---------- Suivi de la diffusion vers les clients ----------
CREATE TABLE diffusion.filigrane (
  canal       text PRIMARY KEY,               -- evenements_clients
  dernier_maj timestamp NOT NULL
);

-- ---------- Rôles ----------
CREATE ROLE chargeur    LOGIN PASSWORD 'chargeur';     -- chargeur Python : écrit dans raw
CREATE ROLE lecture_bi  LOGIN PASSWORD 'lecture_bi';   -- Metabase, Power BI, Qlik : lit core et marts
CREATE ROLE portail_ref LOGIN PASSWORD 'portail_ref';  -- NocoDB : modifie les référentiels
CREATE ROLE authenticator LOGIN NOINHERIT PASSWORD 'authenticator';  -- PostgREST
CREATE ROLE web_anon      NOLOGIN;                     -- appel sans jeton : aucun droit
CREATE ROLE portail_client NOLOGIN;                    -- appel avec jeton client
GRANT web_anon, portail_client TO authenticator;

GRANT USAGE ON SCHEMA raw TO chargeur;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA raw TO chargeur;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA raw TO chargeur;
GRANT USAGE ON SCHEMA ref TO portail_ref, lecture_bi;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ref TO portail_ref;
GRANT SELECT ON ALL TABLES IN SCHEMA ref TO lecture_bi;
GRANT USAGE ON SCHEMA ia TO lecture_bi;
ALTER DEFAULT PRIVILEGES IN SCHEMA ia GRANT SELECT ON TABLES TO lecture_bi;
