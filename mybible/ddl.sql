CREATE EXTENSION IF NOT EXISTS plsh;
CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE EXTENSION IF NOT EXISTS sqlite_fdw;

CREATE SCHEMA IF NOT EXISTS registry;

CREATE TABLE IF NOT EXISTS registry.registry_t (
    version INT NOT NULL PRIMARY KEY,
    json JSONB NOT NULL
);


CREATE OR REPLACE FUNCTION registry.current() RETURNS INT AS $SQL$
  SELECT version FROM registry.registry_t;
$SQL$ LANGUAGE SQL STABLE;
--select registry.current();

 
CREATE TABLE IF NOT EXISTS registry.downloads_status (
  abr TEXT NOT NULL,
  downloaded BOOLEAN NOT NULL DEFAULT FALSE,
  errors TEXT,
  PRIMARY KEY (abr)   
);
--select * from registry.downloads_status where downloaded

DO $$ BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_type WHERE typcategory='E' AND typname='module_type') THEN
    CREATE TYPE registry.module_type AS ENUM (
      'bible', 'dictionary', 'commentaries', 'plan', 
      'devotions', 'crossreferences', 'subheadings'
    );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS registry.modules_status (
  module TEXT NOT NULL PRIMARY KEY,
  abr TEXT NOT NULL,
  mtype registry.module_type NOT NULL,
  attached BOOLEAN NOT NULL DEFAULT FALSE,
  attach_error TEXT,
  valid BOOLEAN NOT NULL DEFAULT FALSE,
  validation_error TEXT
);

DROP MATERIALIZED VIEW IF EXISTS registry.downloads_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS registry.downloads_mv AS
  WITH
  hosts AS (
    SELECT h.alias, h.path, h.priority, h.weight
    FROM registry.registry_t t,
      JSON_TABLE(t.json,
        '$.hosts[*]' COLUMNS (
          alias TEXT PATH '$.alias',
          path TEXT PATH '$.path',
          priority TEXT PATH '$.priority',
          weight TEXT PATH '$.weight'
      )) h),
  _urls AS (
    SELECT d.abr, 
      RIGHT(SPLIT_PART(d.mask,'}',1),-1) alias, 
      SPLIT_PART(d.mask,'}',2) fname
    FROM registry.registry_t t,
      JSON_TABLE(t.json,
        '$.downloads[*]' COLUMNS (
          abr TEXT PATH '$.abr',
          fil TEXT PATH '$.fil',
          NESTED PATH '$.url[*]' COLUMNS (
            mask TEXT PATH '$'
      ))) d),
  urls AS (
    SELECT u.abr, 
      ARRAY_AGG(REPLACE(h.path, '%s', u.fname) ORDER BY h.priority) urls 
    FROM hosts h JOIN _urls u ON h.alias = u.alias
    GROUP BY u.abr),
  downloads AS (
    SELECT t.version, d.*
    FROM registry.registry_t t,
      JSON_TABLE(t.json,
        '$.downloads[*]' COLUMNS (
          abr TEXT    PATH '$.abr',
          aln TEXT    PATH '$.aln',
          reg TEXT    PATH '$.reg',
          des TEXT    PATH '$.des',
          lds JSONB   PATH '$.lds',
          inf TEXT    PATH '$.inf',
          fil TEXT    PATH '$.fil',
          upd TEXT    PATH '$.upd',
          cmt TEXT    PATH '$.cmt',
          def BOOLEAN PATH '$.def',
          siz TEXT    PATH '$.siz',
          hid BOOLEAN PATH '$.hid'
      )) d)
  SELECT d.*, u.urls
  FROM downloads d LEFT JOIN urls u ON d.abr = u.abr;
CREATE UNIQUE INDEX uq_downloads_mv_abr ON registry.downloads_mv(abr);
--select * from registry.downloads_mv;

CREATE OR REPLACE FUNCTION wget_version() RETURNS TEXT AS $PLSH$
#!/bin/sh
wget -qO - http://mph4.ru/registry_info.json
$PLSH$ LANGUAGE plsh;

CREATE OR REPLACE FUNCTION wget_registry() RETURNS TEXT AS $PLSH$
#!/bin/sh
wget -qO - http://mph4.ru/registry.zip | gunzip
$PLSH$ LANGUAGE plsh;


CREATE OR REPLACE PROCEDURE registry.download_registry(version INT)
LANGUAGE plpgsql AS $PLPGSQL$
#variable_conflict use_variable
DECLARE
  reg JSONB;
BEGIN
  IF EXISTS (SELECT 1 FROM registry.registry_t r WHERE r.version = version) THEN
    RAISE INFO 'REGISTRY ALREADY DOWNLOADED';
    RETURN;
  END IF;

  SELECT RIGHT(wget_registry(), -1)::JSONB INTO STRICT reg;

  IF version != (reg->'version')::INT THEN
    RAISE EXCEPTION 'WRONG VERSION.';
  END IF;

  INSERT INTO registry.registry_t(version, json) VALUES((reg->'version')::INT, reg);

  REFRESH MATERIALIZED VIEW registry.downloads_mv;

  INSERT INTO registry.downloads_status(abr)
  SELECT abr FROM registry.downloads_mv
  ON CONFLICT DO NOTHING;
END
$PLPGSQL$; 
--select * from registry.registry_t

CREATE OR REPLACE PROCEDURE registry.download_module(url TEXT, path TEXT, fil TEXT)
LANGUAGE plpython3u AS $PLPYTHON$ 
from io import BytesIO
from urllib.request import urlopen
from urllib.parse import quote
from zipfile import ZipFile
with urlopen(url.replace(' ', '%20').replace('◇', quote('◇'))) as zipresp:
  with ZipFile(BytesIO(zipresp.read())) as zfile:
    for zi in zfile.infolist():
      if zi.filename.startswith('.'):
        zi.filename = fil+zi.filename
      zfile.extract(zi, path)
$PLPYTHON$;

CREATE OR REPLACE PROCEDURE registry.download_modules(versions_path TEXT)
LANGUAGE plpgsql AS $PLPGSQL$
#variable_conflict use_variable
DECLARE
  d registry.downloads_mv%ROWTYPE;
  url TEXT;
  err TEXT;
BEGIN
  FOR d IN 
    SELECT dd.*
    FROM registry.downloads_mv dd
    JOIN registry.downloads_status ds ON dd.abr = ds.abr
    WHERE NOT ds.downloaded
  LOOP
    err = NULL;
    FOREACH url IN ARRAY d.urls LOOP
      BEGIN
        raise notice '%', d.abr;
        CALL registry.download_module(url, versions_path||'/'||d.version, d.fil);
        err = NULL;
        EXIT;
      EXCEPTION WHEN OTHERS THEN
        raise notice 'ERROR %', SQLERRM;
        err = COALESCE(err,'') || SQLERRM; 
      END;
    END LOOP;
    UPDATE registry.downloads_status AS ds SET downloaded = err IS NULL, errors = err
    WHERE ds.abr = d.abr;
    COMMIT;
  END LOOP;
END
$PLPGSQL$;

CREATE OR REPLACE FUNCTION registry.ls_modules(versions_path TEXT, version INT)
RETURNS SETOF TEXT AS $PLPYTHON$
from glob import iglob
from pathlib import Path
for n in iglob('*.SQLite3', root_dir=Path(versions_path) / str(version)):
  yield Path(n).stem
$PLPYTHON$ LANGUAGE plpython3u;

CREATE OR REPLACE PROCEDURE registry.attach_modules(versions_path TEXT)
LANGUAGE plpgsql AS $PLPGSQL$
#variable_conflict use_variable
DECLARE
  module TEXT;
  abr TEXT;
  mtype registry.module_type;
  version INT;
  r registry.modules_status%ROWTYPE;
  err TEXT;
BEGIN
  SELECT t.version INTO STRICT version FROM registry.registry_t t;
  INSERT INTO registry.modules_status (module, abr, mtype)
  SELECT m module, split_part(m, '.', 1) abr,
    (CASE WHEN m LIKE '%.%' THEN split_part(m, '.', -1) ELSE 'bible' END)::registry.module_type mtype
  FROM registry.ls_modules(versions_path, version) m
  ON CONFLICT DO NOTHING;

  FOR r IN SELECT * FROM registry.modules_status WHERE NOT attached LOOP
    module = r.module; abr = r.abr; mtype = r.mtype; err = NULL;
    raise notice '% = % + %', module, abr, mtype;

    BEGIN
      EXECUTE FORMAT(
        'DROP SERVER IF EXISTS %I CASCADE', module);
      EXECUTE FORMAT(
        'DROP SCHEMA IF EXISTS %I CASCADE', module);
      EXECUTE FORMAT(
        'CREATE SERVER %I TYPE %L VERSION %L FOREIGN DATA WRAPPER sqlite_fdw OPTIONS (database %L);',
        module, mtype, version, concat(versions_path,'/',version,'/',module,'.SQLite3'));
      EXECUTE FORMAT(
        'CREATE SCHEMA %I;', module);
      EXECUTE FORMAT(
        'IMPORT FOREIGN SCHEMA public FROM SERVER %I INTO %I;', module, module);
    EXCEPTION WHEN OTHERS THEN
      raise notice 'ERROR %', SQLERRM;
      err = COALESCE(err,'') || SQLERRM; 
    END;

    UPDATE registry.modules_status AS ms SET attached = err IS NULL, attach_error = err
    WHERE ms.module = module;
    COMMIT;

  END LOOP;
END
$PLPGSQL$;
--select * FROM information_schema.foreign_servers
--select * from registry.modules_status where attached;