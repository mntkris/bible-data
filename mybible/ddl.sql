CREATE EXTENSION IF NOT EXISTS plsh;
CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE EXTENSION IF NOT EXISTS sqlite_fdw;

CREATE SCHEMA IF NOT EXISTS registry;

CREATE SCHEMA IF NOT EXISTS bible;
CREATE SCHEMA IF NOT EXISTS dictionary;
CREATE SCHEMA IF NOT EXISTS commentaries;
CREATE SCHEMA IF NOT EXISTS plan;
CREATE SCHEMA IF NOT EXISTS devotions;
CREATE SCHEMA IF NOT EXISTS crossreferences;
CREATE SCHEMA IF NOT EXISTS subheadings;

CREATE TABLE IF NOT EXISTS registry.registry_t (
    version INT NOT NULL PRIMARY KEY,
    json JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION registry.current() RETURNS INT AS $SQL$
  SELECT version FROM registry.registry_t;
$SQL$ LANGUAGE SQL STABLE;
 
CREATE TABLE IF NOT EXISTS registry.downloads_status (
  abr TEXT NOT NULL,
  downloaded BOOLEAN NOT NULL DEFAULT FALSE,
  errors TEXT,
  PRIMARY KEY (abr)   
);

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

CREATE OR REPLACE PROCEDURE registry.validate_bible_modules()
LANGUAGE plpgsql AS $PLPGSQL$
#variable_conflict use_variable
DECLARE
  module TEXT;
  r RECORD;
  err TEXT;
  q TEXT;
BEGIN
  FOR module IN 
    SELECT ms.module FROM registry.modules_status ms 
    WHERE ms.attached AND ms.mtype = 'bible' 
    limit 20000
  LOOP
    raise info '%', module;
    err = NULL;
    
    BEGIN -- info
      EXECUTE FORMAT('SELECT MAX(name), MAX(value) FROM %I.info', module) INTO STRICT r;
      EXECUTE FORMAT('SELECT name FROM %I.info GROUP BY name HAVING COUNT(*) > 1 LIMIT 1', module) INTO r;
      IF r IS NOT NULL THEN RAISE EXCEPTION 'Not unique info: %', r.name; END IF;
    EXCEPTION WHEN OTHERS THEN 
      err = concat_ws(E'\n', COALESCE(err, ''), '----- info ------', SQLERRM);
    END;

    BEGIN -- books
      q = FORMAT(
        'SELECT MAX(book_number::INT), MAX(book_color), MAX(short_name), MAX(long_name), MAX(%s::INT) FROM %I.books',
        COALESCE((SELECT c.column_name FROM information_schema.columns c 
                  WHERE (c.table_schema, c.table_name, c.column_name) = (module, 'books', 'sorting_order')), 'NULL'),
        module);
      EXECUTE q INTO STRICT r;
      EXECUTE FORMAT('SELECT book_number FROM %I.books GROUP BY book_number HAVING COUNT(*) > 1 LIMIT 1', module) INTO r;
      IF r IS NOT NULL THEN RAISE EXCEPTION 'Not unique books: %', r.book_number; END IF;
    EXCEPTION WHEN OTHERS THEN 
      err = concat_ws(E'\n', COALESCE(err, ''), '----- books ------', SQLERRM);
    END;

    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE (t.table_schema, t.table_name) = (module, 'books_all')) THEN
      BEGIN  -- books_all
        q = FORMAT(
          'SELECT MAX(book_number::INT), MAX(book_color), MAX(short_name), MAX(%s), '
          '       MAX(long_name), MAX(is_present::INT), MAX(%s::INT) FROM %I.books_all',
          COALESCE((SELECT c.column_name FROM information_schema.columns c 
                    WHERE (c.table_schema, c.table_name, c.column_name) = (module, 'books_all', 'title')), 'long_name'),
          COALESCE((SELECT c.column_name FROM information_schema.columns c 
                    WHERE (c.table_schema, c.table_name, c.column_name) = (module, 'books_all', 'sorting_order')), 'NULL'),
          module);
        EXECUTE q INTO STRICT r;
        EXECUTE FORMAT('SELECT book_number FROM %I.books_all GROUP BY book_number HAVING COUNT(*) > 1 LIMIT 1', module) INTO r;
        IF r IS NOT NULL THEN RAISE EXCEPTION 'Not unique books_all: %', r.book_number; END IF;
      EXCEPTION WHEN OTHERS THEN 
        err = concat_ws(E'\n', COALESCE(err, ''), '----- books_all ------', SQLERRM);
      END;
    END IF;
    
    BEGIN  --  verses
      EXECUTE FORMAT(
        'SELECT MAX(book_number::INT), MAX(chapter::INT), MAX(verse::INT), MAX(text) FROM %I.verses',
        module) INTO STRICT r;
      EXECUTE FORMAT('SELECT book_number, chapter, verse FROM %I.verses GROUP BY book_number, chapter, verse HAVING COUNT(*) > 1 LIMIT 1', module) INTO r;
      IF r IS NOT NULL THEN RAISE EXCEPTION 'Not unique verses: % % %', r.book_number, r.chapter, r.verse; END IF;
    EXCEPTION WHEN OTHERS THEN 
      err = concat_ws(E'\n', COALESCE(err, ''), '----- verses ------', SQLERRM);
    END;

    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE (t.table_schema, t.table_name) = (module, 'introductions')) THEN
      BEGIN
        EXECUTE FORMAT('SELECT MAX(book_number::INT), MAX(introduction) FROM %I.introductions', module) INTO STRICT r;
      EXCEPTION WHEN OTHERS THEN 
        err = concat_ws(E'\n', COALESCE(err, ''), '----- introductions ------', SQLERRM);
      END;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE (t.table_schema, t.table_name) = (module, 'stories')) THEN
      BEGIN
        q = FORMAT(
          'SELECT MAX(book_number::INT), MAX(chapter::INT), MAX(verse::INT), MAX(%s::INT), MAX(%s::TEXT) FROM %I.stories',
          COALESCE((SELECT c.column_name FROM information_schema.columns c 
                    WHERE (c.table_schema, c.table_name, c.column_name) = (module, 'stories', 'order_if_several')), 'NULL'),
          COALESCE((SELECT c.column_name FROM information_schema.columns c 
                    WHERE (c.table_schema, c.table_name, c.column_name) = (module, 'stories', 'title')), 'NULL'),
          module);
        EXECUTE q INTO STRICT r;
        -- TODO unique if necessary
      EXCEPTION WHEN OTHERS THEN 
        err = concat_ws(E'\n', COALESCE(err, ''), '----- stories ------', SQLERRM);
      END;
    END IF;
        
    IF EXISTS (SELECT 1 FROM information_schema.tables t WHERE (t.table_schema, t.table_name) = (module, 'morphology_indications')) THEN
      BEGIN
        EXECUTE FORMAT(
          'SELECT MAX(indication), MAX(applicable_to), MAX(language), MAX(meaning) FROM %I.morphology_indications',
          module) INTO STRICT r;
        -- TODO unique if necessary
      EXCEPTION WHEN OTHERS THEN 
        err = concat_ws(E'\n', COALESCE(err, ''), '----- morphology_indications ------', SQLERRM);
      END;
    END IF;

    -- morphology_topics probably not used 

    UPDATE registry.modules_status AS ms SET valid = err IS NULL, validation_error = err
    WHERE ms.module = module;
    COMMIT;

  END LOOP; 

END
$PLPGSQL$;

CREATE OR REPLACE PROCEDURE registry.accumulate_bible_modules()
LANGUAGE plpgsql AS $PLPGSQL$
#variable_conflict use_variable
DECLARE
  q TEXT;
BEGIN

  SELECT
    'CREATE MATERIALIZED VIEW IF NOT EXISTS bible.info_mv AS
    ' || STRING_AGG(FORMAT(
            '  SELECT %L abr, name::TEXT, value::TEXT FROM %I.info',
            ms.abr, ms.module
          ), E' UNION ALL\n') || ';' INTO STRICT q
  FROM registry.modules_status ms
  JOIN information_schema.tables t ON t.table_schema = ms.module
  WHERE ms.valid AND ms.mtype = 'bible' AND t.table_name = 'info';
  DROP MATERIALIZED VIEW IF EXISTS bible.info_mv; EXECUTE q;
  CREATE UNIQUE INDEX uq__bible__info_mv__name ON bible.info_mv(abr, name);
  COMMIT;

  SELECT
    'CREATE MATERIALIZED VIEW IF NOT EXISTS bible.books_mv AS
    ' || STRING_AGG(FORMAT(
            '  SELECT %L abr, book_number::INT, book_color::TEXT, short_name::TEXT, long_name::TEXT, %s::INT sorting_order FROM %I.books',
            ms.abr, COALESCE(c.column_name, 'NULL'), ms.module
          ), E' UNION ALL\n') || ';' INTO STRICT q
  FROM registry.modules_status ms
  JOIN information_schema.tables t ON t.table_schema = ms.module
  LEFT JOIN information_schema.columns c ON (c.table_schema, c.table_name, c.column_name) = (t.table_schema, 'books', 'sorting_order')
  WHERE ms.valid AND ms.mtype = 'bible' AND t.table_name = 'books';
  DROP MATERIALIZED VIEW IF EXISTS bible.books_mv; EXECUTE q;
  CREATE UNIQUE INDEX uq__bible__books_mv__book_number ON bible.books_mv(abr, book_number);
  COMMIT;

  SELECT
    'CREATE MATERIALIZED VIEW IF NOT EXISTS bible.books_all_mv AS
    ' || STRING_AGG(FORMAT(
            '  SELECT %L abr, book_number::INT, book_color::TEXT, short_name::TEXT, %s::TEXT title, long_name::TEXT, is_present::INT, %s::INT sorting_order FROM %I.books_all',
            ms.abr, COALESCE(c1.column_name, 'NULL'), COALESCE(c2.column_name, 'NULL'), ms.module
          ), E' UNION ALL\n') || ';' INTO STRICT q
  FROM registry.modules_status ms
  JOIN information_schema.tables t ON t.table_schema = ms.module
  LEFT JOIN information_schema.columns c1 ON (c1.table_schema, c1.table_name, c1.column_name) = (t.table_schema, 'books_all', 'title')
  LEFT JOIN information_schema.columns c2 ON (c2.table_schema, c2.table_name, c2.column_name) = (t.table_schema, 'books_all', 'sorting_order')
  WHERE ms.valid AND ms.mtype = 'bible' AND t.table_name = 'books_all';
  DROP MATERIALIZED VIEW IF EXISTS bible.books_all_mv; EXECUTE q;
  CREATE UNIQUE INDEX uq__bible__books_all_mv__book_number ON bible.books_all_mv(abr, book_number);
  COMMIT;

  SELECT
    'CREATE MATERIALIZED VIEW IF NOT EXISTS bible.verses_mv AS
    ' || STRING_AGG(FORMAT(
        '  SELECT %L abr, book_number::INT, chapter::INT, verse::INT, text::TEXT FROM %I.verses',
            ms.abr, ms.module
          ), E' UNION ALL\n') || ';' INTO STRICT q
  FROM registry.modules_status ms
  JOIN information_schema.tables t ON t.table_schema = ms.module
  WHERE ms.valid AND ms.mtype = 'bible' AND t.table_name = 'verses';
  DROP MATERIALIZED VIEW IF EXISTS bible.verses_mv; EXECUTE q;
  CREATE UNIQUE INDEX uq__bible__verses_mv__book_numer__chapter__verse ON bible.verses_mv(abr, book_number, chapter, verse);
  COMMIT;

  SELECT
    'CREATE MATERIALIZED VIEW IF NOT EXISTS bible.stories_mv AS
    ' || STRING_AGG(FORMAT(
        '  SELECT %L abr, book_number::INT, chapter::INT, verse::INT, %s::INT order_if_several, %s::TEXT title FROM %I.stories',
            ms.abr, COALESCE(c1.column_name, 'NULL'), COALESCE(c2.column_name, 'NULL'), ms.module
          ), E' UNION ALL\n') || ';' INTO STRICT q
  FROM registry.modules_status ms
  JOIN information_schema.tables t ON t.table_schema = ms.module
  LEFT JOIN information_schema.columns c1 ON (c1.table_schema, c1.table_name, c1.column_name) = (t.table_name, 'stories', 'order_if_several')
  LEFT JOIN information_schema.columns c2 ON (c2.table_schema, c2.table_name, c2.column_name) = (t.table_name, 'stories', 'title')
  WHERE ms.valid AND ms.mtype = 'bible' AND t.table_name = 'stories';
  DROP MATERIALIZED VIEW IF EXISTS bible.stories_mv; EXECUTE q;
  CREATE UNIQUE INDEX uq__bible__stories_mv__book_number__chapter__verse__order_if 
    ON bible.stories_mv(abr, book_number, chapter, verse, order_if_several);
  COMMIT;

  SELECT
    'CREATE MATERIALIZED VIEW IF NOT EXISTS bible.morphology_indications_mv AS
    ' || STRING_AGG(FORMAT(
        '  SELECT %L abr, indication::TEXT, applicable_to::TEXT, language::TEXT, meaning::TEXT FROM %I.morphology_indications',
            ms.abr, ms.module
          ), E' UNION ALL\n') || ';' INTO STRICT q
  FROM registry.modules_status ms
  JOIN information_schema.tables t ON t.table_schema = ms.module
  WHERE ms.valid AND ms.mtype = 'bible' AND t.table_name = 'morphology_indications';
  DROP MATERIALIZED VIEW IF EXISTS bible.morphology_indications_mv; EXECUTE q;
  CREATE UNIQUE INDEX uq__bible__morphology_indications_mv__indication_applicable_lg 
    ON bible.morphology_indications_mv(abr, indication, applicable_to, language);
  COMMIT;

END
$PLPGSQL$;
