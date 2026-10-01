-- Bases sources simulées : deux WMS de conception différente, un TMS,
-- l'outil de ticketing et le portail de saisie « DONUT ».

CREATE SCHEMA wms_nord;   -- WMS du site de Lille (codes clients « CLI001 »)
CREATE TABLE wms_nord.receptions (
  id             bigserial PRIMARY KEY,
  num_reception  text NOT NULL UNIQUE,
  code_client    text NOT NULL,
  date_prevue    date NOT NULL,
  date_reception timestamp,
  statut         text NOT NULL,          -- ATTENDUE, EN_COURS, CLOTUREE
  nb_palettes    int  NOT NULL,
  maj_le         timestamp NOT NULL DEFAULT now()
);
CREATE TABLE wms_nord.commandes (
  id              bigserial PRIMARY KEY,
  num_commande    text NOT NULL UNIQUE,
  code_client     text NOT NULL,
  date_creation   timestamp NOT NULL,
  date_expedition timestamp,
  statut          text NOT NULL,         -- CREEE, PREPAREE, EXPEDIEE, ANNULEE
  nb_lignes       int  NOT NULL,
  nb_colis        int,
  maj_le          timestamp NOT NULL DEFAULT now()
);
CREATE TABLE wms_nord.stock (
  code_client text NOT NULL,
  sku         text NOT NULL,
  quantite    int  NOT NULL,
  date_photo  date NOT NULL,
  maj_le      timestamp NOT NULL DEFAULT now(),
  PRIMARY KEY (code_client, sku, date_photo)
);

CREATE SCHEMA wms_sud;    -- WMS du site de Lyon : autre éditeur, autres noms et codes
CREATE TABLE wms_sud.inbound (
  inb_id      bigserial PRIMARY KEY,
  ref         text NOT NULL UNIQUE,
  customer    text NOT NULL,               -- codes « C-001 »
  expected_at date NOT NULL,
  received_at timestamp,
  status      int  NOT NULL,               -- 10 attendue, 20 en cours, 30 clôturée
  pallets     int  NOT NULL,
  updated_at  timestamp NOT NULL DEFAULT now()
);
CREATE TABLE wms_sud.outbound (
  out_id     bigserial PRIMARY KEY,
  ref        text NOT NULL UNIQUE,
  customer   text NOT NULL,
  created_at timestamp NOT NULL,
  shipped_at timestamp,
  status     int  NOT NULL,                -- 10 créée, 40 préparée, 60 expédiée, 90 annulée
  lines      int  NOT NULL,
  parcels    int,
  updated_at timestamp NOT NULL DEFAULT now()
);
CREATE TABLE wms_sud.inventory (
  customer      text NOT NULL,
  item          text NOT NULL,
  qty           int  NOT NULL,
  snapshot_date date NOT NULL,
  updated_at    timestamp NOT NULL DEFAULT now(),
  PRIMARY KEY (customer, item, snapshot_date)
);

CREATE SCHEMA tms;
CREATE TABLE tms.expeditions (
  id             bigserial PRIMARY KEY,
  num_expedition text NOT NULL UNIQUE,
  num_commande   text NOT NULL,
  site           text NOT NULL,            -- LIL, LYO
  code_client    text NOT NULL,            -- code du WMS d'origine
  transporteur   text NOT NULL,            -- GEODIS, DPD
  date_depart    timestamp NOT NULL,
  poids_kg       numeric(10,2) NOT NULL,
  nb_colis       int NOT NULL,
  cout_ht        numeric(10,2) NOT NULL,
  maj_le         timestamp NOT NULL DEFAULT now()
);

CREATE SCHEMA ticketing;
CREATE TABLE ticketing.tickets (
  id          bigserial PRIMARY KEY,
  code_client text NOT NULL,
  site        text NOT NULL,
  categorie   text NOT NULL,               -- LITIGE, RETARD, CASSE, QUESTION
  priorite    text NOT NULL,               -- P1, P2, P3
  ouvert_le   timestamp NOT NULL,
  resolu_le   timestamp,
  statut      text NOT NULL,               -- OUVERT, RESOLU
  maj_le      timestamp NOT NULL DEFAULT now()
);

CREATE SCHEMA donut;      -- portail web de saisie manuelle
CREATE TABLE donut.saisies (
  id          bigserial PRIMARY KEY,
  code_client text NOT NULL,
  type_saisie text NOT NULL,               -- PRESTATION_ANNEXE
  reference   text NOT NULL,
  valeur      numeric(10,2) NOT NULL,
  saisi_le    timestamp NOT NULL,
  maj_le      timestamp NOT NULL DEFAULT now()
);

-- Compte de lecture utilisé par le chargeur (principe du moindre privilège)
CREATE ROLE extraction LOGIN PASSWORD 'extraction';
GRANT USAGE ON SCHEMA wms_nord, wms_sud, tms, ticketing, donut TO extraction;
GRANT SELECT ON ALL TABLES IN SCHEMA wms_nord, wms_sud, tms, ticketing, donut TO extraction;
