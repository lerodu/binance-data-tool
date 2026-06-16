"""Storage sinks. Faithful, per-source tables: a parquet file's columns are
written verbatim (nested list/struct columns JSON-encoded to TEXT; nothing
renamed or harmonized). Sinks are interchangeable behind one interface so the
storage backend can change without touching ingest.

  SqliteSink   -> a local .sqlite (matches the backend's relational world)
  CsvDumpSink  -> one CSV per table + schema.sql (portable: SQLite .import / Postgres copy)
  PostgresSink -> deferred (same interface; add when the cloud DB exists)
"""

import csv
import json
import os
import time
from abc import ABC, abstractmethod

import pyarrow as pa
import pyarrow.types as pat


def arrow_sqltype(t):
    """Map a pyarrow type to a dialect-neutral SQL type."""
    if pat.is_integer(t) or pat.is_boolean(t):
        return "INTEGER"
    if pat.is_floating(t) or pat.is_decimal(t):
        return "REAL"
    return "TEXT"  # strings, timestamps, nested list/struct (JSON-encoded)


def _scalar(v):
    """Normalize a python value from arrow .to_pylist() into a SQL-storable scalar."""
    if v is None or isinstance(v, (int, float, str)):
        return v
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (list, dict)):
        return json.dumps(v, separators=(",", ":"), default=str)
    return str(v)  # datetime, Decimal, bytes -> text


def normalize_rows(table: "pa.Table"):
    """(column names, list of row tuples) with nested cells JSON-encoded."""
    cols = table.column_names
    rows = [tuple(_scalar(r[c]) for c in cols) for r in table.to_pylist()]
    return cols, rows


class Sink(ABC):
    @abstractmethod
    def ensure_table(self, table, schema): ...        # schema: pyarrow.Schema
    @abstractmethod
    def insert(self, table, table_data): ...          # table_data: pyarrow.Table
    @abstractmethod
    def is_done(self, source, datatype, key) -> bool: ...
    @abstractmethod
    def mark_done(self, source, datatype, key, nrows): ...
    @abstractmethod
    def finalize(self, meta): ...
    def close(self): ...


class SqliteSink(Sink):
    def __init__(self, db_path):
        import sqlite3
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
        self.con = sqlite3.connect(db_path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("CREATE TABLE IF NOT EXISTS ingest_meta "
                         "(source TEXT, datatype TEXT, key TEXT, rows INTEGER, built_ms INTEGER, "
                         "PRIMARY KEY(source,datatype,key))")
        self._tables = set()

    def ensure_table(self, table, schema):
        if table in self._tables:
            return
        cols = ", ".join(f'"{f.name}" {arrow_sqltype(f.type)}' for f in schema)
        self.con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols})')
        self._tables.add(table)

    def insert(self, table, table_data):
        cols, rows = normalize_rows(table_data)
        if not rows:
            return
        ph = ",".join("?" * len(cols))
        cn = ",".join(f'"{c}"' for c in cols)
        self.con.executemany(f'INSERT INTO "{table}" ({cn}) VALUES ({ph})', rows)

    def is_done(self, source, datatype, key):
        cur = self.con.execute("SELECT 1 FROM ingest_meta WHERE source=? AND datatype=? AND key=?",
                               (source, datatype, key))
        return cur.fetchone() is not None

    def mark_done(self, source, datatype, key, nrows):
        self.con.execute("INSERT OR REPLACE INTO ingest_meta VALUES (?,?,?,?,?)",
                         (source, datatype, key, nrows, int(time.time() * 1000)))
        self.con.commit()

    def finalize(self, meta):
        self.con.commit()

    def close(self):
        self.con.close()


class CsvDumpSink(Sink):
    """One CSV per table + a schema.sql, for import into SQLite (.import) or
    Postgres (\\copy). Resumability via a done.json key set."""

    def __init__(self, out_dir):
        self.out = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self._writers = {}      # table -> (file, csv.writer)
        self._schemas = {}      # table -> [(name, sqltype)]
        self._done_path = os.path.join(out_dir, "done.json")
        self._done = set(tuple(x) for x in json.load(open(self._done_path))) \
            if os.path.exists(self._done_path) else set()

    def ensure_table(self, table, schema):
        if table in self._writers:
            return
        self._schemas[table] = [(f.name, arrow_sqltype(f.type)) for f in schema]
        path = os.path.join(self.out, f"{table}.csv")
        new = not os.path.exists(path)
        f = open(path, "a", newline="")
        w = csv.writer(f)
        if new:
            w.writerow([n for n, _ in self._schemas[table]])
        self._writers[table] = (f, w)

    def insert(self, table, table_data):
        _, w = self._writers[table]
        _, rows = normalize_rows(table_data)
        w.writerows(rows)

    def is_done(self, source, datatype, key):
        return (source, datatype, key) in self._done

    def mark_done(self, source, datatype, key, nrows):
        self._done.add((source, datatype, key))

    def finalize(self, meta):
        for f, _ in self._writers.values():
            f.flush()
        with open(os.path.join(self.out, "schema.sql"), "w") as s:
            for table, cols in self._schemas.items():
                coldefs = ",\n  ".join(f'"{n}" {t}' for n, t in cols)
                s.write(f'CREATE TABLE IF NOT EXISTS "{table}" (\n  {coldefs}\n);\n\n')
        json.dump([list(x) for x in self._done], open(self._done_path, "w"))

    def close(self):
        for f, _ in self._writers.values():
            f.close()


class PostgresSink(Sink):
    def __init__(self, dsn):
        raise NotImplementedError(
            "PostgresSink is deferred. Use --sink csv to produce schema.sql + CSVs and "
            "load them into Postgres with \\copy, until the cloud DB is provisioned.")


def make_sink(name, db=None, out=None):
    if name == "sqlite":
        return SqliteSink(db or "hl.sqlite")
    if name == "csv":
        return CsvDumpSink(out or "hl_dump")
    if name == "postgres":
        return PostgresSink(out)
    raise SystemExit(f"unknown sink {name!r}; choices: sqlite, csv")
