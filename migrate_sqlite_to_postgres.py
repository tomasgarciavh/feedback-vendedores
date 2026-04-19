#!/usr/bin/env python3
"""
Copia todos los datos de feedback.db (SQLite) a PostgreSQL (DATABASE_URL).
Ejecutar una vez desde la raíz del proyecto:
  python migrate_sqlite_to_postgres.py
"""
from __future__ import annotations

import os
import sqlite3
import sys

from dotenv import load_dotenv

load_dotenv()

import psycopg2
from psycopg2.extras import execute_batch

# Import después de dotenv
import database  # noqa: E402


SQLITE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedback.db")

TABLES_ORDER = [
    "vendors",
    "processed_files",
    "roleplay_leads",
    "roleplay_sessions",
    "lanzamiento_submissions",
    "lanzamiento_coach_sessions",
    "vendor_gamification",
    "lanzamiento_kpi_entries",
]

SERIAL_TABLES = [
    ("vendors", "id"),
    ("roleplay_leads", "id"),
    ("roleplay_sessions", "id"),
    ("lanzamiento_coach_sessions", "id"),
    ("lanzamiento_kpi_entries", "id"),
]


def _sqlite_columns(sqlite_conn: sqlite3.Connection, table: str) -> list[str]:
    cur = sqlite_conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _table_exists_sqlite(sqlite_conn: sqlite3.Connection, table: str) -> bool:
    row = sqlite_conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _truncate_pg(raw_pg):
    with raw_pg.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                lanzamiento_kpi_entries,
                vendor_gamification,
                lanzamiento_coach_sessions,
                roleplay_sessions,
                roleplay_leads,
                lanzamiento_submissions,
                processed_files,
                vendors
            RESTART IDENTITY CASCADE
            """
        )
    raw_pg.commit()


def _sync_sequence(raw_pg, table: str, id_col: str):
    with raw_pg.cursor() as cur:
        cur.execute(f"SELECT MAX({id_col}) FROM {table}")
        row = cur.fetchone()
        m = row[0] if row else None
        if m is not None and m > 0:
            cur.execute(
                "SELECT setval(pg_get_serial_sequence(%s, %s), %s)",
                (table, id_col, m),
            )
        else:
            cur.execute(
                "SELECT setval(pg_get_serial_sequence(%s, %s), 1, false)",
                (table, id_col),
            )
    raw_pg.commit()


def main() -> int:
    if not os.path.isfile(SQLITE_PATH):
        print(f"No se encontró {SQLITE_PATH}", file=sys.stderr)
        return 1

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    database.init_db()

    url = database._database_url()
    raw_pg = psycopg2.connect(url)

    print("Truncando tablas en PostgreSQL…")
    _truncate_pg(raw_pg)

    for table in TABLES_ORDER:
        if not _table_exists_sqlite(sqlite_conn, table):
            print(f"  (omitir {table}: no existe en SQLite)")
            continue
        cols = _sqlite_columns(sqlite_conn, table)
        if not cols:
            continue
        rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            print(f"  {table}: 0 filas")
            continue

        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

        tuples = []
        for row in rows:
            tuples.append(tuple(row[c] for c in cols))

        with raw_pg.cursor() as cur:
            execute_batch(cur, sql, tuples, page_size=500)
        raw_pg.commit()
        print(f"  {table}: {len(tuples)} filas")

    for table, id_col in SERIAL_TABLES:
        _sync_sequence(raw_pg, table, id_col)
        print(f"Secuencia {table}.{id_col} actualizada.")

    sqlite_conn.close()
    raw_pg.close()
    print("Migración terminada.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
