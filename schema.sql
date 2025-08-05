-- A SQL schema that you can import network tester CSV into for data mining.

-- You can import CSV data like so:
-- COPY upload_tester.uploads FROM '/Users/alice/results/uploads.csv' WITH (FORMAT CSV, HEADER);
-- COPY upload_tester.sources FROM '/Users/alice/results/sources.csv' WITH (FORMAT CSV, HEADER);
-- COPY upload_tester.shards FROM '/Users/alice/results/shards.csv' WITH (FORMAT CSV, HEADER);
-- COPY retrieval_tester.retrievals FROM '/Users/alice/results/retrievals.csv' WITH (FORMAT CSV, HEADER);

CREATE SCHEMA IF NOT EXISTS upload_tester;

CREATE TABLE upload_tester.uploads (
  ended   TIMESTAMP,
  error   TEXT,
  id      UUID PRIMARY KEY,
  index   TEXT,
  root    TEXT,
  shards  TEXT,
  source  UUID,
  started TIMESTAMP,
  upload  UUID
);

CREATE TABLE upload_tester.sources (
  count    INTEGER,
  created  TIMESTAMP,
  id       UUID PRIMARY KEY,
  region   TEXT,
  size     BIGINT,
  type     TEXT
);

CREATE TABLE upload_tester.shards (
  ended               TIMESTAMP,
  error               TEXT,
  id                  TEXT PRIMARY KEY,
  locationCommitment TEXT,
  node                TEXT,
  size                BIGINT,
  source              UUID,
  started             TIMESTAMP,
  upload              UUID,
  url                 TEXT
);

CREATE SCHEMA IF NOT EXISTS retrieval_tester;

CREATE TABLE retrieval_tester.retrievals (
  ended   TIMESTAMP,
  error   TEXT,
  id      UUID PRIMARY KEY,
  node    TEXT,
  region  TEXT,
  shard   TEXT,
  size    INTEGER,
  slice   TEXT,
  source  UUID,
  started TIMESTAMP,
  status  INTEGER,
  upload  UUID
);


