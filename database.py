from __future__ import annotations

import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _database_url() -> str:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL no está definido. Configuralo en .env (PostgreSQL)."
        )
    if "sslmode" not in url and "rlwy.net" in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url


class _Result:
    __slots__ = ("_cur", "rowcount", "lastrowid")

    def __init__(self, cursor):
        self._cur = cursor
        self.rowcount = cursor.rowcount
        self.lastrowid = None

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()


class _Conn:
    """Adaptador estilo sqlite3 sobre psycopg2 (placeholders ? → %s, EXCLUDED)."""

    def __init__(self, raw):
        self._raw = raw

    def execute(self, sql, params=None):
        sql = sql.replace("?", "%s").replace("excluded.", "EXCLUDED.")
        cur = self._raw.cursor()
        cur.execute(sql, params or ())
        return _Result(cur)

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._raw.rollback()
        self._raw.close()
        return False


def get_connection():
    conn = psycopg2.connect(_database_url(), cursor_factory=RealDictCursor)
    return _Conn(conn)


def _now() -> str:
    return datetime.now(TZ).isoformat()


def _today() -> str:
    return datetime.now(TZ).date().isoformat()


def init_db():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_id TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                vendor_name TEXT NOT NULL,
                vendor_email TEXT,
                processed_at TEXT,
                status TEXT NOT NULL DEFAULT 'processing',
                error_message TEXT,
                feedback_text TEXT,
                score DOUBLE PRECISION,
                section_scores TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vendors (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                photo_path TEXT,
                role TEXT,
                phone TEXT,
                bio TEXT,
                objectives TEXT,
                achievements TEXT,
                results TEXT,
                experience TEXT,
                status TEXT,
                joined_program TEXT,
                metrics TEXT,
                testimonial_video TEXT,
                pin TEXT,
                kpi_vendor INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS roleplay_leads (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                avatar TEXT,
                description TEXT,
                personality TEXT,
                objections TEXT,
                difficulty TEXT DEFAULT 'medio',
                is_system INTEGER DEFAULT 0,
                created_by_vendor_id INTEGER,
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS roleplay_sessions (
                id SERIAL PRIMARY KEY,
                vendor_id INTEGER NOT NULL,
                lead_id INTEGER NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                messages_json TEXT DEFAULT '[]',
                feedback_text TEXT,
                score DOUBLE PRECISION,
                section_scores TEXT,
                status TEXT DEFAULT 'active'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lanzamiento_submissions (
                file_id TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                vendor_name TEXT NOT NULL,
                submitted_at TEXT,
                status TEXT NOT NULL DEFAULT 'processing',
                error_message TEXT,
                feedback_text TEXT,
                score DOUBLE PRECISION,
                section_scores TEXT,
                file_type TEXT DEFAULT 'video',
                analysis_phase TEXT,
                custom_instructions TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lanzamiento_coach_sessions (
                id SERIAL PRIMARY KEY,
                vendor_id INTEGER NOT NULL,
                mode TEXT NOT NULL DEFAULT 'roleplay',
                phase TEXT NOT NULL DEFAULT 'relacion',
                lead_name TEXT,
                lead_context TEXT,
                started_at TEXT,
                ended_at TEXT,
                messages_json TEXT DEFAULT '[]',
                feedback_text TEXT,
                score DOUBLE PRECISION,
                status TEXT DEFAULT 'active'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vendor_gamification (
                vendor_id INTEGER PRIMARY KEY,
                xp INTEGER DEFAULT 0,
                streak INTEGER DEFAULT 0,
                last_activity_date TEXT,
                badges_json TEXT DEFAULT '[]'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lanzamiento_kpi_entries (
                id SERIAL PRIMARY KEY,
                vendor_id INTEGER NOT NULL,
                entry_date TEXT NOT NULL,
                no_respondido INTEGER DEFAULT 0,
                interaccion_leve_frio INTEGER DEFAULT 0,
                interaccion_leve_tibio INTEGER DEFAULT 0,
                interaccion_leve_caliente INTEGER DEFAULT 0,
                conversacion_fluida_frio INTEGER DEFAULT 0,
                conversacion_fluida_tibio INTEGER DEFAULT 0,
                conversacion_fluida_caliente INTEGER DEFAULT 0,
                potencial_compra_frio INTEGER DEFAULT 0,
                potencial_compra_tibio INTEGER DEFAULT 0,
                potencial_compra_caliente INTEGER DEFAULT 0,
                venta_realizada INTEGER DEFAULT 0,
                notes TEXT,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(vendor_id, entry_date)
            )
        """)
        conn.commit()
    logger.info("Database initialized.")


# ── Lanzamiento submissions ─────────────────────────────────────────────────

def lanzamiento_mark_processing(file_id: str, file_name: str, vendor_name: str, file_type: str = "video"):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO lanzamiento_submissions (file_id, file_name, vendor_name, submitted_at, status, file_type)
            VALUES (?, ?, ?, ?, 'processing', ?)
            ON CONFLICT(file_id) DO UPDATE SET
                status = 'processing',
                submitted_at = excluded.submitted_at,
                error_message = NULL
            """,
            (file_id, file_name, vendor_name, _now(), file_type),
        )
        conn.commit()


def lanzamiento_mark_done(file_id: str, feedback_text: str = "", score: float = None, section_scores: str = None):
    with get_connection() as conn:
        conn.execute(
            """UPDATE lanzamiento_submissions
               SET status = 'done', submitted_at = ?, error_message = NULL,
                   feedback_text = ?, score = ?, section_scores = ?
               WHERE file_id = ?""",
            (_now(), feedback_text, score, section_scores, file_id),
        )
        conn.commit()


def lanzamiento_mark_error(file_id: str, error_message: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE lanzamiento_submissions SET status = 'error', submitted_at = ?, error_message = ? WHERE file_id = ?",
            (_now(), error_message[:1000], file_id),
        )
        conn.commit()


def lanzamiento_get_recent(limit: int = 100):
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM lanzamiento_submissions ORDER BY submitted_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def lanzamiento_get_by_vendor(vendor_name: str, limit: int = 50):
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM lanzamiento_submissions WHERE vendor_name = ? ORDER BY submitted_at DESC LIMIT ?",
            (vendor_name, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def is_processed(file_id: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT status FROM processed_files WHERE file_id = ?", (file_id,)
        ).fetchone()
    return row is not None and row["status"] in ("done", "processing")


def mark_processing(file_id: str, file_name: str, vendor_name: str, vendor_email: str = None):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO processed_files (file_id, file_name, vendor_name, vendor_email, processed_at, status)
            VALUES (?, ?, ?, ?, ?, 'processing')
            ON CONFLICT(file_id) DO UPDATE SET
                status = 'processing',
                vendor_email = excluded.vendor_email,
                processed_at = excluded.processed_at,
                error_message = NULL
            """,
            (file_id, file_name, vendor_name, vendor_email, _now()),
        )
        conn.commit()


def mark_done(file_id: str, feedback_text: str = "", score: float = None, section_scores: str = None):
    with get_connection() as conn:
        conn.execute(
            """UPDATE processed_files
               SET status = 'done', processed_at = ?, error_message = NULL,
                   feedback_text = ?, score = ?, section_scores = ?
               WHERE file_id = ?""",
            (_now(), feedback_text, score, section_scores, file_id),
        )
        conn.commit()


def mark_error(file_id: str, error_message: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE processed_files SET status = 'error', processed_at = ?, error_message = ? WHERE file_id = ?",
            (_now(), error_message[:1000], file_id),
        )
        conn.commit()


def get_recent_records(limit: int = 100) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT p.file_id, p.file_name, p.vendor_name, p.vendor_email,
                   p.processed_at, p.status, p.error_message, p.feedback_text, p.score, p.section_scores,
                   v.id as vendor_id
            FROM processed_files p
            LEFT JOIN vendors v ON LOWER(TRIM(v.name)) = LOWER(TRIM(p.vendor_name))
            ORDER BY p.processed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_vendor_records(vendor_name: str) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT file_id, file_name, vendor_name, processed_at, status,
                   feedback_text, score, section_scores
            FROM processed_files
            WHERE vendor_name = ? AND status = 'done'
            ORDER BY processed_at ASC
            """,
            (vendor_name,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_vendors() -> list:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, email, photo_path, role, status, joined_program FROM vendors ORDER BY name"
        ).fetchall()
    return [dict(row) for row in rows]


def get_vendor_by_id(vendor_id: int):
    with get_connection() as conn:
        row = conn.execute(
            """SELECT id, name, email, photo_path, role, phone, bio, objectives,
                      achievements, results, experience, status, joined_program, metrics
               FROM vendors WHERE id = ?""", (vendor_id,)
        ).fetchone()
    return dict(row) if row else None


def update_vendor_info(vendor_id: int, role: str = None, phone: str = None, bio: str = None,
                       objectives: str = None, achievements: str = None, results: str = None,
                       experience: str = None, status: str = None, joined_program: str = None,
                       metrics: str = None):
    with get_connection() as conn:
        conn.execute(
            """UPDATE vendors SET role=?, phone=?, bio=?, objectives=?,
               achievements=?, results=?, experience=?, status=?, joined_program=?, metrics=?
               WHERE id=?""",
            (role or None, phone or None, bio or None, objectives or None,
             achievements or None, results or None, experience or None,
             status or None, joined_program or None, metrics or None, vendor_id),
        )
        conn.commit()


def upsert_vendor(name: str, email: str):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO vendors (name, email) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET email = excluded.email
            """,
            (name.strip(), email.strip()),
        )
        conn.commit()


def update_vendor_testimonial(vendor_id: int, filename: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE vendors SET testimonial_video = ? WHERE id = ?",
            (filename, vendor_id),
        )
        conn.commit()


def update_vendor_photo(vendor_id: int, photo_path: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE vendors SET photo_path = ? WHERE id = ?",
            (photo_path, vendor_id),
        )
        conn.commit()


def delete_vendor(vendor_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM vendors WHERE id = ?", (vendor_id,))
        conn.commit()


def get_vendor_email(vendor_name: str) -> str | None:
    normalized = vendor_name.strip().lower()
    with get_connection() as conn:
        rows = conn.execute("SELECT name, email FROM vendors").fetchall()
    for row in rows:
        if row["name"].strip().lower() == normalized:
            return row["email"]
    return None


def get_analytics_data() -> dict:
    """Returns aggregated data for the analytics dashboard."""
    section_keys = [
        "diagnostico_desapego", "descubrimiento_acuerdos", "empatia_escucha",
        "ingenieria_preguntas", "gestion_creencias", "storytelling",
        "pitch_personalizado", "mentalidad",
    ]
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT p.vendor_name, p.score, p.section_scores, p.processed_at,
                   v.id as vendor_id
            FROM processed_files p
            LEFT JOIN vendors v ON LOWER(TRIM(v.name)) = LOWER(TRIM(p.vendor_name))
            WHERE p.status = 'done' AND p.score IS NOT NULL
            ORDER BY p.processed_at ASC
            """,
        ).fetchall()

    from collections import defaultdict
    import json as _json

    by_vendor = defaultdict(list)
    for row in rows:
        by_vendor[row["vendor_name"]].append(dict(row))

    vendors_stats = []
    global_section_sums = {k: 0.0 for k in section_keys}
    global_section_counts = {k: 0 for k in section_keys}

    for vendor_name, recs in sorted(by_vendor.items()):
        scores = [r["score"] for r in recs]
        avg = round(sum(scores) / len(scores), 1)
        best = max(scores)
        vendor_id = recs[0]["vendor_id"]

        section_avgs = {}
        for k in section_keys:
            vals = []
            for r in recs:
                if r["section_scores"]:
                    try:
                        sec = _json.loads(r["section_scores"])
                        vals.append(float(sec.get(k, 0)))
                    except Exception:
                        pass
            if vals:
                v = round(sum(vals) / len(vals), 1)
                section_avgs[k] = v
                global_section_sums[k] += sum(vals)
                global_section_counts[k] += len(vals)

        # trend
        trend = None
        if len(scores) >= 2:
            trend = "up" if scores[-1] > scores[-2] else ("down" if scores[-1] < scores[-2] else "equal")

        vendors_stats.append({
            "name": vendor_name,
            "vendor_id": vendor_id,
            "sessions": len(recs),
            "avg_score": avg,
            "best_score": best,
            "last_score": scores[-1],
            "section_avgs": section_avgs,
            "trend": trend,
            "dates": [r["processed_at"][:10] for r in recs],
            "scores": scores,
        })

    # Sort by avg desc
    vendors_stats.sort(key=lambda x: x["avg_score"], reverse=True)

    # Global section averages
    global_section_avgs = {}
    for k in section_keys:
        if global_section_counts[k]:
            global_section_avgs[k] = round(global_section_sums[k] / global_section_counts[k], 1)
        else:
            global_section_avgs[k] = 0.0

    total_sessions = sum(v["sessions"] for v in vendors_stats)
    all_scores = [r["score"] for rows_list in by_vendor.values() for r in rows_list]
    global_avg = round(sum(all_scores) / len(all_scores), 1) if all_scores else 0

    return {
        "vendors": vendors_stats,
        "global_section_avgs": global_section_avgs,
        "total_sessions": total_sessions,
        "global_avg": global_avg,
        "total_vendors": len(vendors_stats),
        "section_keys": section_keys,
    }


def reset_stuck_processing() -> int:
    """Marca como error los registros que quedaron en 'processing' de sesiones anteriores."""
    with get_connection() as conn:
        cur = conn.execute(
            """UPDATE processed_files SET status = 'error', error_message = 'Proceso interrumpido al reiniciar el servidor.'
               WHERE status = 'processing'"""
        )
        conn.commit()
    return cur.rowcount


def count_today() -> int:
    today = _today()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM processed_files WHERE status = 'done' AND processed_at LIKE ?",
            (f"{today}%",),
        ).fetchone()
    return row["cnt"] if row else 0


def delete_record(file_id: str):
    with get_connection() as conn:
        conn.execute("DELETE FROM processed_files WHERE file_id = ?", (file_id,))
        conn.commit()


def count_pending() -> int:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM processed_files WHERE status = 'processing'"
        ).fetchone()
    return row["cnt"] if row else 0


# ── Roleplay ──────────────────────────────────────────────────────────────────

def get_vendor_by_name(name: str):
    """Auth for vendor chat login — name only, no PIN required."""
    with get_connection() as conn:
        rows = conn.execute("SELECT id, name, pin, photo_path FROM vendors").fetchall()
    for row in rows:
        if row["name"].strip().lower() == name.strip().lower():
            return dict(row)
    return None


def get_vendor_by_name_and_pin(name: str, pin: str):
    """Auth for vendor chat login (legacy — kept for compatibility)."""
    with get_connection() as conn:
        rows = conn.execute("SELECT id, name, pin, photo_path FROM vendors").fetchall()
    for row in rows:
        if row["name"].strip().lower() == name.strip().lower():
            stored_pin = row["pin"] or "1234"
            if stored_pin == pin:
                return dict(row)
    return None


def update_vendor_pin(vendor_id: int, pin: str):
    with get_connection() as conn:
        conn.execute("UPDATE vendors SET pin=? WHERE id=?", (pin, vendor_id))
        conn.commit()


def get_system_leads() -> list:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM roleplay_leads WHERE is_system=1 ORDER BY difficulty, name"
        ).fetchall()
    return [dict(r) for r in rows]


def get_vendor_leads(vendor_id: int) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM roleplay_leads WHERE created_by_vendor_id=? ORDER BY created_at DESC",
            (vendor_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_lead_by_id(lead_id: int):
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM roleplay_leads WHERE id=?", (lead_id,)).fetchone()
    return dict(row) if row else None


def create_lead(name: str, description: str, personality: str, objections: str,
                difficulty: str, avatar: str, is_system: int = 0,
                vendor_id: int = None) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO roleplay_leads
               (name, description, personality, objections, difficulty, avatar, is_system, created_by_vendor_id, created_at)
               VALUES (?,?,?,?,?,?,?,?,?)
               RETURNING id""",
            (name, description, personality, objections, difficulty, avatar,
             is_system, vendor_id, _now())
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"] if row else None


def delete_lead(lead_id: int, vendor_id: int):
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM roleplay_leads WHERE id=? AND created_by_vendor_id=? AND is_system=0",
            (lead_id, vendor_id)
        )
        conn.commit()


def create_roleplay_session(vendor_id: int, lead_id: int) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO roleplay_sessions (vendor_id, lead_id, started_at, messages_json, status)
               VALUES (?,?,?,'[]','active')
               RETURNING id""",
            (vendor_id, lead_id, _now())
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"] if row else None


def get_session(session_id: int):
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM roleplay_sessions WHERE id=?", (session_id,)).fetchone()
    return dict(row) if row else None


def update_session_messages(session_id: int, messages_json: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE roleplay_sessions SET messages_json=? WHERE id=?",
            (messages_json, session_id)
        )
        conn.commit()


def close_session(session_id: int, feedback_text: str, score: float, section_scores: str):
    with get_connection() as conn:
        conn.execute(
            """UPDATE roleplay_sessions
               SET status='done', ended_at=?, feedback_text=?, score=?, section_scores=?
               WHERE id=?""",
            (_now(), feedback_text, score, section_scores, session_id)
        )
        conn.commit()


# ── Lanzamiento Coach Sessions ──────────────────────────────────────────────

def lanzamiento_coach_create(vendor_id: int, mode: str, phase: str,
                              lead_name: str = "", lead_context: str = "") -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO lanzamiento_coach_sessions
               (vendor_id, mode, phase, lead_name, lead_context, started_at, messages_json, status)
               VALUES (?,?,?,?,?,?,'[]','active')
               RETURNING id""",
            (vendor_id, mode, phase, lead_name, lead_context, _now())
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"] if row else None


def lanzamiento_coach_get(session_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM lanzamiento_coach_sessions WHERE id=?", (session_id,)
        ).fetchone()
    return dict(row) if row else None


def lanzamiento_coach_update_messages(session_id: int, messages_json: str):
    with get_connection() as conn:
        conn.execute(
            "UPDATE lanzamiento_coach_sessions SET messages_json=? WHERE id=?",
            (messages_json, session_id)
        )
        conn.commit()


def lanzamiento_coach_close(session_id: int, feedback_text: str = "", score: float = None):
    with get_connection() as conn:
        conn.execute(
            """UPDATE lanzamiento_coach_sessions
               SET status='done', ended_at=?, feedback_text=?, score=?
               WHERE id=?""",
            (_now(), feedback_text, score, session_id)
        )
        conn.commit()


def lanzamiento_coach_get_vendor_sessions(vendor_id: int) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM lanzamiento_coach_sessions
               WHERE vendor_id=? ORDER BY started_at DESC LIMIT 50""",
            (vendor_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_vendor_sessions(vendor_id: int) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT s.*, l.name as lead_name, l.avatar as lead_avatar, l.difficulty
               FROM roleplay_sessions s
               JOIN roleplay_leads l ON l.id = s.lead_id
               WHERE s.vendor_id=?
               ORDER BY s.started_at DESC""",
            (vendor_id,)
        ).fetchall()
    return [dict(r) for r in rows]


# ── Gamification ────────────────────────────────────────────────────────────

BADGES = {
    "primer_disparo":      {"emoji": "🎯", "nombre": "Primer Disparo",       "desc": "Completaste tu primer roleplay"},
    "en_llamas":           {"emoji": "🔥", "nombre": "En Llamas",            "desc": "3 días seguidos practicando"},
    "imparable":           {"emoji": "⚡", "nombre": "Imparable",            "desc": "7 días seguidos practicando"},
    "diez_rodadas":        {"emoji": "💪", "nombre": "10 Rodadas",           "desc": "Completaste 10 roleplays"},
    "veinticinco":         {"emoji": "🌟", "nombre": "25 Roleplays",         "desc": "Completaste 25 roleplays"},
    "perfeccionista":      {"emoji": "🏆", "nombre": "Perfeccionista",       "desc": "Obtuviste 9 o más en un roleplay"},
    "elite_vh":            {"emoji": "👑", "nombre": "Elite VH",             "desc": "Completaste todos los leads del sistema"},
    "cazador_objeciones":  {"emoji": "🛡️", "nombre": "Cazador de Objeciones","desc": "Venciste los 3 leads difíciles"},
    "promedio_ocho":       {"emoji": "⭐", "nombre": "Promedio 8+",          "desc": "Promedio ≥ 8 con al menos 5 sesiones"},
    "el_valiente":         {"emoji": "🦁", "nombre": "El Valiente",          "desc": "Practicaste con el lead más difícil"},
}

LEVELS = [
    (0,    "Principiante", "🌱"),
    (100,  "Aprendiz",     "📚"),
    (250,  "Vendedor",     "💼"),
    (500,  "Vendedor Pro", "⭐"),
    (800,  "Experto VH",   "🏆"),
    (1200, "Elite VH",     "👑"),
]

def get_level_info(xp: int) -> dict:
    level_name, level_emoji, next_xp = "Principiante", "🌱", 100
    for i, (threshold, name, emoji) in enumerate(LEVELS):
        if xp >= threshold:
            level_name = name
            level_emoji = emoji
            next_xp = LEVELS[i + 1][0] if i + 1 < len(LEVELS) else None
    return {"name": level_name, "emoji": level_emoji, "next_xp": next_xp}


def get_gamification(vendor_id: int) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM vendor_gamification WHERE vendor_id=?", (vendor_id,)
        ).fetchone()
    if not row:
        return {"vendor_id": vendor_id, "xp": 0, "streak": 0,
                "last_activity_date": None, "badges_json": "[]"}
    return dict(row)


def _ensure_gamification_row(conn, vendor_id: int):
    conn.execute(
        """INSERT INTO vendor_gamification
           (vendor_id, xp, streak, last_activity_date, badges_json)
           VALUES (?,0,0,NULL,'[]')
           ON CONFLICT (vendor_id) DO NOTHING""",
        (vendor_id,)
    )


def award_xp_and_badges(vendor_id: int, session_score: float | None, lead_id: int) -> dict:
    """Called after a roleplay session ends. Returns {xp_gained, new_badges, level_up}."""
    import json as _json
    today = _today()

    with get_connection() as conn:
        _ensure_gamification_row(conn, vendor_id)
        row = dict(conn.execute(
            "SELECT * FROM vendor_gamification WHERE vendor_id=?", (vendor_id,)
        ).fetchone())

        # --- XP calculation ---
        xp_gained = 50  # base por completar
        if session_score:
            if session_score >= 9:  xp_gained += 25
            elif session_score >= 8: xp_gained += 15
            elif session_score >= 7: xp_gained += 5

        # Bonus primera vez con este lead
        prev = conn.execute(
            """SELECT COUNT(*) as c FROM roleplay_sessions
               WHERE vendor_id=? AND lead_id=? AND status='done'""",
            (vendor_id, lead_id)
        ).fetchone()["c"]
        if prev == 0:
            xp_gained += 10

        new_xp = row["xp"] + xp_gained

        # --- Streak calculation ---
        last = row["last_activity_date"]
        streak = row["streak"] or 0
        if last == today:
            pass  # ya contó hoy
        elif last and (datetime.fromisoformat(today) - datetime.fromisoformat(last)).days == 1:
            streak += 1
        else:
            streak = 1

        # --- Badge checking ---
        badges = _json.loads(row["badges_json"] or "[]")
        new_badges = []

        done_count = conn.execute(
            "SELECT COUNT(*) as c FROM roleplay_sessions WHERE vendor_id=? AND status='done'",
            (vendor_id,)
        ).fetchone()["c"] + 1  # +1 porque esta sesión aún no se marcó done antes del commit

        # Estadísticas de scores
        scores = [r["score"] for r in conn.execute(
            "SELECT score FROM roleplay_sessions WHERE vendor_id=? AND status='done' AND score IS NOT NULL",
            (vendor_id,)
        ).fetchall()]
        if session_score: scores.append(session_score)

        # Leads difíciles completados
        difficult_done = conn.execute(
            """SELECT DISTINCT s.lead_id FROM roleplay_sessions s
               JOIN roleplay_leads l ON l.id=s.lead_id
               WHERE s.vendor_id=? AND s.status='done' AND l.difficulty='difícil'""",
            (vendor_id,)
        ).fetchall()
        difficult_ids = {r["lead_id"] for r in difficult_done}
        difficult_leads = conn.execute(
            "SELECT id FROM roleplay_leads WHERE is_system=1 AND difficulty='difícil'"
        ).fetchall()
        all_difficult_ids = {r["id"] for r in difficult_leads}

        # Todos los leads del sistema completados
        system_leads = conn.execute(
            "SELECT id FROM roleplay_leads WHERE is_system=1"
        ).fetchall()
        done_lead_ids = {r["lead_id"] for r in conn.execute(
            "SELECT DISTINCT lead_id FROM roleplay_sessions WHERE vendor_id=? AND status='done'",
            (vendor_id,)
        ).fetchall()} | {lead_id}

        # Hardest lead (highest id of system difficult)
        hardest = conn.execute(
            "SELECT id FROM roleplay_leads WHERE is_system=1 AND difficulty='difícil' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        hardest_id = hardest["id"] if hardest else None

        def check_badge(key, condition):
            if key not in badges and condition:
                badges.append(key)
                new_badges.append(key)

        check_badge("primer_disparo",     done_count >= 1)
        check_badge("diez_rodadas",       done_count >= 10)
        check_badge("veinticinco",        done_count >= 25)
        check_badge("perfeccionista",     session_score and session_score >= 9)
        check_badge("en_llamas",          streak >= 3)
        check_badge("imparable",          streak >= 7)
        check_badge("el_valiente",        hardest_id and lead_id == hardest_id)
        check_badge("cazador_objeciones", all_difficult_ids and all_difficult_ids <= (difficult_ids | {lead_id}))
        check_badge("elite_vh",           {r["id"] for r in system_leads} <= done_lead_ids)
        check_badge("promedio_ocho",      len(scores) >= 5 and (sum(scores)/len(scores)) >= 8)

        old_level = get_level_info(row["xp"])
        new_level = get_level_info(new_xp)
        level_up = new_level["name"] != old_level["name"]

        conn.execute(
            """UPDATE vendor_gamification
               SET xp=?, streak=?, last_activity_date=?, badges_json=?
               WHERE vendor_id=?""",
            (new_xp, streak, today, _json.dumps(badges), vendor_id)
        )
        conn.commit()

    return {
        "xp_gained": xp_gained,
        "total_xp": new_xp,
        "streak": streak,
        "new_badges": [{"key": k, **BADGES[k]} for k in new_badges],
        "level_up": level_up,
        "new_level": new_level if level_up else None,
        "level": new_level,
    }


def seed_system_leads():
    """Inserta o actualiza los leads predeterminados según el avatar real de compradores VH."""
    # Siempre actualiza/inserta para mantener los leads al día con el avatar real
    leads = [
        # ── BLOQUE 1: Avatar principal — La Empleada que quiere independencia ──
        {
            "name": "Elena — La Empleada del Estado",
            "avatar": "🏛️",
            "difficulty": "fácil",
            "description": "Empleada pública 35 años, quiere independencia y cobrar en dólares. Tiene 2 emprendimientos con su pareja. Dolor claro.",
            "personality": """Sos Elena, 35 años, empleada del Estado hace 8 años. Ganás un sueldo fijo en pesos que ya no alcanza. Tenés 2 emprendimientos pequeños con tu pareja (venden ropa y hacen tortas por encargo) pero ninguno despegó. Te sumaste al taller de VH porque 'ya no querés depender más de eso' y porque 'si podés cobrar en dólares, mejor'. Tu frase al sumarte fue 'me sumé para adquirir algunas herramientas q me puedan ayudar'. Estás abierta al diálogo, respondés rápido, pero tenés miedo de invertir mal porque ya gastaste plata en cosas que no funcionaron. Cuando te preguntan por tu situación, contás todo. Hablás en español argentino informal, usás minúsculas y abreviaturas típicas de WhatsApp ('xq', 'qiero', 'tmb'). Respondés en 1-2 oraciones.""",
            "objections": "No sé si tengo el tiempo con mis emprendimientos. ¿Esto sirve también si tengo negocio propio? ¿Y si ya trabajé vendiendo? ¿Cuánto sale?",
        },
        {
            "name": "Daniela — El Lead Caliente",
            "avatar": "🔥",
            "difficulty": "fácil",
            "description": "Está lista para comprar. Ya vio testimonios, ya está convencida. Si el vendedor la apura o no conecta, se enfría.",
            "personality": """Sos Daniela, 31 años, coach de bienestar con muchas ganas de escalar tu negocio. Ya viste testimonios, ya sabés del programa, ESTÁS LISTA para entrar. Pero si el vendedor no te escucha, te apura o te parece un script robótico, te enfriás y decís 'bueno, lo pienso'. Al inicio sos entusiasta: 'me interesa mucho', 'ya vi testimonios', '¿cómo arrancamos?'. Si el vendedor conecta bien con vos y te escucha de verdad, cerrás rápido. Si te apura con el precio o suena a vendehumo, bajás la guardia. Respondés en 1-3 oraciones, cálida pero atenta a cómo te tratan.""",
            "objections": "Si el vendedor me apura: 'dejame pensarlo'. Si no conecta: '¿me podés contar más de los resultados de alumnos reales?'.",
        },
        {
            "name": "Sofía — La que Perdió la Clase",
            "avatar": "😅",
            "difficulty": "fácil",
            "description": "Se perdió la clase 2 del taller por un conflicto de horario. Pide la grabación, sigue interesada pero su timing es diferente.",
            "personality": """Sos Sofía, 33 años, asistente administrativa. Te inscribiste al taller gratis de VH pero te perdiste la Clase 2 porque coincidía con las clases de tu otro curso. Cuando el vendedor te contacta, lo primero que preguntás es '¿quedó grabada la clase de ayer?'. Estás interesada pero no podés comprometerte a ver todo en vivo, y necesitás acceso asincrónico. Si te ofrecen la grabación rápido, agradecés mucho. También preguntás si 'en el programa se puede ver todo grabado'. Tenés interés genuino pero tu momento no es ahora — probablemente preguntes por el próximo lanzamiento si el precio te parece mucho. Hablás en español argentino, cálida, 1-2 oraciones.""",
            "objections": "¿Quedó grabada? No puedo ver todo en vivo. ¿Cuánto sale? ¿Hay otro lanzamiento después si no entro ahora?",
        },
        # ── BLOQUE 2: Objeciones clásicas ──
        {
            "name": "María — La Indecisa",
            "avatar": "🤔",
            "difficulty": "medio",
            "description": "Quiere pero posterga. Su frase es 'lo tengo que pensar'. Emprendedora estancada hace 2 años.",
            "personality": """Sos María, emprendedora de 35 años que vende bijouterie artesanal por redes sociales. Estás estancada en los mismos ingresos hace 2 años (~$300.000/mes ARS). Ves el valor del programa, PERO siempre buscás razones para no decidir ahora. Frases típicas: 'no sé, lo tengo que pensar', 'me parece bien pero dame unos días', '¿me podés mandar la info por WhatsApp?'. El miedo real es gastar plata y que no funcione — una vez pagaste un curso y no terminaste ni el 30%. Respondés en 1-2 oraciones, amable pero esquiva.""",
            "objections": "Lo tengo que pensar. Dame unos días. Mandame la info por mensaje. No sé si es el momento.",
        },
        {
            "name": "Lucía — Sin Presupuesto",
            "avatar": "💸",
            "difficulty": "medio",
            "description": "Empleada con emprendimiento chico. El precio la frena, pero genuinamente no tiene plata ahorrada.",
            "personality": """Sos Lucía, 29 años, empleada y con un emprendimiento de repostería los fines de semana. Ganás ~$180.000/mes y el programa te parece caro. Sí querés crecer, sí ves el valor, PERO genuinamente el precio te parece mucho y no tenés ahorros. Creés que si pudieras vender mejor tus tortas ya tendríais el dinero, pero es un círculo. Frases típicas: 'es caro para lo que gano', 'no me llega', '¿hay cuotas con Mercado Pago?', '¿se puede pagar una parte con lo que gane después?'. Respondés amablemente, 1-2 oraciones.""",
            "objections": "Es caro. No me llega el dinero. ¿Hay cuotas? ¿Se puede pagar con comisiones como dicen?",
        },
        {
            "name": "Roberto — El Muy Ocupado",
            "avatar": "⏰",
            "difficulty": "medio",
            "description": "Empresario con 12 empleados. Todo le parece bien pero dice que no tiene tiempo.",
            "personality": """Sos Roberto, 48 años, dueño de una empresa de logística con 12 empleados. Estás MUY ocupado, siempre con el teléfono sonando. El programa te interesa pero creés que no tenés tiempo. En realidad el problema es que no delegás nada y vivís apagando incendios. Frases típicas: 'no tengo tiempo para clases', '¿cuántas horas por semana lleva?', 'ya sé que debería pero…'. A veces te vas en medio del mensaje: 'perdón, un segundo'. Si el vendedor te entiende de verdad, abrís la guardia. 1-2 oraciones.""",
            "objections": "No tengo tiempo. ¿Cuántas horas lleva? ¿Se puede ver grabado? No puedo sumar más cosas.",
        },
        {
            "name": "Diego — El Referido",
            "avatar": "🤝",
            "difficulty": "medio",
            "description": "Llegó por referido de un amigo de Tomás. Alta confianza inicial, pero quiere entender bien el ecosistema antes de comprar.",
            "personality": """Sos Diego, 30 años, community manager freelance. Te contactó el vendedor porque tu amigo Nehuén te lo recomendó — Nehuén es amigo personal de Tomás. Llegás con alta confianza: 'Nehuén me dijo que era muy bueno'. Querés entender bien qué incluye antes de comprometerte. Preguntás mucho sobre el 'ecosistema': '¿se puede entrar en distintas áreas de la academia?', '¿cuánto tiempo dura la bolsa de trabajo?', '¿qué pasa si necesito más acompañamiento después?'. No sos difícil, pero querés que te lo expliquen bien. 1-2 oraciones, curioso y amable.""",
            "objections": "¿Qué incluye exactamente? ¿Por cuánto tiempo dura el acceso? ¿Puedo entrar en más áreas de la academia?",
        },
        # ── BLOQUE 3: Leads con alta resistencia ──
        {
            "name": "Carlos — El Escéptico",
            "avatar": "🙄",
            "difficulty": "difícil",
            "description": "Vendedor experimentado que ya probó 3 cursos que no le funcionaron. Pide evidencia concreta.",
            "personality": """Sos Carlos, 42 años, vendedor B2B con 15 años de experiencia. Ya pagaste 3 cursos de ventas y ninguno te cambió los resultados. Estás cansado de las promesas. Sos directo, un poco cínico, y pedís evidencia concreta: casos reales, números, alumnos que puedas contactar. Frases típicas: 'todos prometen lo mismo', '¿cuánto facturó el alumno promedio?', 'ya probé X y no me sirvió', '¿me podés dar el contacto de un alumno para preguntarle?'. Respondés en 1-2 oraciones, directo. Solo bajás la guardia si te dan prueba social real y específica.""",
            "objections": "Todos dicen lo mismo. Ya probé otros. Dame resultados reales con números. ¿Podés conectarme con un alumno?",
        },
        {
            "name": "Silvina — Consulta con su Pareja",
            "avatar": "👫",
            "difficulty": "difícil",
            "description": "Le encanta pero dice que tiene que consultarlo con su marido, que es escéptico de los cursos online.",
            "personality": """Sos Silvina, 38 años, vendés servicios de diseño gráfico freelance. El programa te copó, pero siempre postergás con 'lo tengo que hablar con Gustavo'. Tu marido es escéptico de los cursos online — la última vez que compraste uno sin consultarle hubo quilombo. Tu miedo real es su reacción. Frases típicas: 'sí me gusta pero lo tengo que hablar con Gustavo', 'él siempre se pone en contra de estos gastos', 'si fuera por mí lo haría ya', '¿podría él hablar con alguien del equipo?'. Respondés cálida pero evasiva, 1-2 oraciones.""",
            "objections": "Lo tengo que hablar con mi marido. Él no sé cómo va a reaccionar. ¿Podría hablar alguien con él?",
        },
        {
            "name": "Matías — El Comparador",
            "avatar": "🔍",
            "difficulty": "difícil",
            "description": "Investigó otros programas durante 3 semanas. Compara precios y propuestas. Analítico y exigente.",
            "personality": """Sos Matías, 33 años, emprendedor digital. Estuviste 3 semanas investigando programas de ventas. Tenés un Excel con comparativos: precios, testimonios, módulos. Comparás todo. Frases típicas: 'vi un programa similar que cuesta la mitad', '¿qué tiene esto que no tenga el de Jürgen Klaric?', '¿por qué vale lo que vale si hay cursos gratis en YouTube?'. Sos analítico, pedís argumentos concretos. Solo te convencés con diferenciación clara y valor específico, no con argumentos genéricos. 1-2 oraciones.""",
            "objections": "Vi otros más baratos. ¿En qué se diferencia? ¿Por qué vale lo que vale? Hay cosas similares más baratas.",
        },
        {
            "name": "Andrés — El Que Sabe Todo",
            "avatar": "🎓",
            "difficulty": "difícil",
            "description": "Gerente comercial con MBA. Cree que ya sabe todo de ventas. Solo baja la guardia si lo desafiás inteligentemente.",
            "personality": """Sos Andrés, 44 años, gerente comercial con MBA. Leíste SPIN Selling, Challenger Sale, Dale Carnegie, Jürgen Klaric, todos los libros. Creés que sabés de ventas — tu problema real es que tu equipo no llega a objetivos pero no lo admitís fácil. Frases típicas: 'eso ya lo sé', 'eso es muy básico', '¿qué tiene de nuevo esto?'. Sos inteligente y desafiante. Solo te bajás la guardia si el vendedor te desafía con algo que genuinamente no sabés o te toca el punto ciego: que saber teoría no es lo mismo que aplicarla. 1-2 oraciones, directo y algo arrogante.""",
            "objections": "Eso ya lo sé. Es muy básico. ¿Qué me enseñaría que no sepa? Ya leí todo sobre ventas.",
        },
    ]

    with get_connection() as conn:
        existing = {
            r["name"] for r in conn.execute(
                "SELECT name FROM roleplay_leads WHERE is_system=1"
            ).fetchall()
        }

    new_leads = [l for l in leads if l["name"] not in existing]
    for l in new_leads:
        create_lead(
            name=l["name"], description=l["description"],
            personality=l["personality"], objections=l["objections"],
            difficulty=l["difficulty"], avatar=l["avatar"], is_system=1
        )
    if new_leads:
        logger.info("Seeded %d new system leads.", len(new_leads))
    else:
        logger.info("System leads already up to date.")
