from __future__ import annotations

import sqlite3
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DB_PATH = "feedback.db"
TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _now() -> str:
    return datetime.now(TZ).isoformat()


def _today() -> str:
    return datetime.now(TZ).date().isoformat()


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
                score REAL,
                section_scores TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vendors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                photo_path TEXT
            )
        """)
        # Migrate existing tables — add columns if they don't exist yet
        for col, typedef in [
            ("score", "REAL"),
            ("section_scores", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE processed_files ADD COLUMN {col} {typedef}")
            except Exception:
                pass
        for col, typedef in [
            ("photo_path", "TEXT"),
            ("role", "TEXT"),
            ("phone", "TEXT"),
            ("bio", "TEXT"),
            ("objectives", "TEXT"),
            ("achievements", "TEXT"),
            ("results", "TEXT"),
            ("experience", "TEXT"),
            ("status", "TEXT"),
            ("joined_program", "TEXT"),
            ("metrics", "TEXT"),
            ("testimonial_video", "TEXT"),
            ("pin", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE vendors ADD COLUMN {col} {typedef}")
            except Exception:
                pass

        # Roleplay leads
        conn.execute("""
            CREATE TABLE IF NOT EXISTS roleplay_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        # Roleplay sessions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS roleplay_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor_id INTEGER NOT NULL,
                lead_id INTEGER NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                messages_json TEXT DEFAULT '[]',
                feedback_text TEXT,
                score REAL,
                section_scores TEXT,
                status TEXT DEFAULT 'active'
            )
        """)
        # Lanzamiento feedback submissions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lanzamiento_submissions (
                file_id TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                vendor_name TEXT NOT NULL,
                submitted_at TEXT,
                status TEXT NOT NULL DEFAULT 'processing',
                error_message TEXT,
                feedback_text TEXT,
                score REAL,
                section_scores TEXT,
                file_type TEXT DEFAULT 'video'
            )
        """)
        # Lanzamiento coach sessions (roleplay + asistente en vivo)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lanzamiento_coach_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor_id INTEGER NOT NULL,
                mode TEXT NOT NULL DEFAULT 'roleplay',
                phase TEXT NOT NULL DEFAULT 'relacion',
                lead_name TEXT,
                lead_context TEXT,
                started_at TEXT,
                ended_at TEXT,
                messages_json TEXT DEFAULT '[]',
                feedback_text TEXT,
                score REAL,
                status TEXT DEFAULT 'active'
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
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (name, description, personality, objections, difficulty, avatar,
             is_system, vendor_id, _now())
        )
        conn.commit()
        return cur.lastrowid


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
               VALUES (?,?,?,'[]','active')""",
            (vendor_id, lead_id, _now())
        )
        conn.commit()
        return cur.lastrowid


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
               VALUES (?,?,?,?,?,?,'[]','active')""",
            (vendor_id, mode, phase, lead_name, lead_context, _now())
        )
        conn.commit()
        return cur.lastrowid


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


def seed_system_leads():
    """Inserta los leads predeterminados si no existen."""
    with get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) as c FROM roleplay_leads WHERE is_system=1").fetchone()["c"]
    if count > 0:
        return

    leads = [
        {
            "name": "María — La Indecisa",
            "avatar": "🤔",
            "difficulty": "fácil",
            "description": "Emprendedora, 35 años. Le interesa el programa pero siempre posterga. Su frase favorita es 'lo tengo que pensar'.",
            "personality": "Eres María, una emprendedora de 35 años que vende bijouterie artesanal por redes sociales. Estás atascada en los mismos ingresos hace 2 años (~$300.000/mes ARS). Ves el valor del programa, PERO siempre buscás razones para no decidir ahora. Frases típicas tuyas: 'no sé, lo tengo que pensar', 'me parece bien pero dame unos días', '¿me podés mandar la info por WhatsApp?'. Respondés con mensajes cortos, 1-2 oraciones. Hablás en español argentino informal. Sos amable pero esquiva.",
            "objections": "Lo tengo que pensar. Dame unos días. Mandame la info por mensaje. No sé si es el momento.",
        },
        {
            "name": "Carlos — El Escéptico",
            "avatar": "🙄",
            "difficulty": "medio",
            "description": "Vendedor con experiencia. Ya probó otros cursos que no le funcionaron. Desconfía de las promesas.",
            "personality": "Eres Carlos, 42 años, vendedor B2B con 15 años de experiencia. Ya pagaste 3 cursos de ventas y ninguno te cambió los resultados. Estás cansado de las promesas. Sos directo, un poco cínico, y pedís evidencia concreta antes de creer algo. Frases típicas: 'eso ya lo sé', 'todos prometen lo mismo', '¿cuánto facturaron tus alumnos?', 'ya probé X y no me sirvió'. Hablás en español argentino, directamente. Respondés con 1-2 oraciones, a veces cortás con una pregunta incómoda.",
            "objections": "Todos dicen lo mismo. Ya probé otros cursos. ¿Tenés resultados reales? ¿Qué tiene de diferente esto?",
        },
        {
            "name": "Lucía — Sin Presupuesto",
            "avatar": "💸",
            "difficulty": "medio",
            "description": "Quiere hacerlo pero dice que no tiene el dinero. Trabaja en relación de dependencia y tiene un emprendimiento chico.",
            "personality": "Eres Lucía, 29 años, empleada en una empresa y con un emprendimiento de repostería los fines de semana. Ganás ~$180.000/mes y el programa te parece caro. Sí querés crecer, sí ves el valor, PERO genuinamente el precio te parece mucho y no tenés ahorros. Frases típicas: 'es caro para lo que gano', 'no me llega', 'tendría que ver si puedo en cuotas', '¿hay alguna beca o descuento?'. Respondés amablemente, en 1-2 oraciones, en español argentino.",
            "objections": "Es caro. No me llega el dinero. ¿Hay cuotas? ¿Descuento? Tendría que ver bien los números.",
        },
        {
            "name": "Roberto — El Muy Ocupado",
            "avatar": "⏰",
            "difficulty": "medio",
            "description": "Empresario con poco tiempo. Todo le parece bien pero dice que no tiene horas para dedicarle al programa.",
            "personality": "Eres Roberto, 48 años, dueño de una empresa de logística con 12 empleados. Estás MUY ocupado, siempre con el teléfono sonando. El programa te interesa pero creés que no tenés tiempo. Frases típicas: 'no tengo tiempo para clases', 'con todo lo que tengo no podría cursarlo', 'ya sé que es bueno pero…', 'cuántas horas por semana lleva'. Respondés en 1-2 oraciones, a veces interrumpís con 'perdón, un segundo' o avisás que tenés que cortar.",
            "objections": "No tengo tiempo. ¿Cuántas horas lleva? Ya sé que debería pero no puedo sumar más cosas.",
        },
        {
            "name": "Silvina — Consulta con su Pareja",
            "avatar": "👫",
            "difficulty": "difícil",
            "description": "Le encanta el programa pero dice que tiene que consultarlo con su marido antes de decidir.",
            "personality": "Eres Silvina, 38 años, vende servicios de diseño gráfico freelance. El programa te copó, pero siempre postergás con 'lo tengo que hablar con mi marido'. En realidad tu marido es escéptico de los cursos online y vos tenés miedo de su reacción. Frases típicas: 'sí me gusta pero lo tengo que hablar con Gustavo', 'él siempre se pone re en contra de estos gastos', 'si fuera por mí lo haría', '¿podría él hablar con alguien del equipo?'. Respondés en 1-2 oraciones, de forma cálida pero evasiva.",
            "objections": "Lo tengo que hablar con mi marido. Él no sé cómo va a reaccionar. No quiero tomar la decisión sola.",
        },
        {
            "name": "Matías — El Comparador",
            "avatar": "🔍",
            "difficulty": "difícil",
            "description": "Estuvo investigando otros programas. Compara precios y propuestas constantemente.",
            "personality": "Eres Matías, 33 años, emprendedor del mundo digital. Estuviste 3 semanas investigando programas de ventas. Tenés comparativos, precios, reviews. Comparás todo con la competencia. Frases típicas: 'vi un programa similar que cuesta la mitad', 'X referente da algo parecido', '¿qué tiene esto que no tenga el de fulano?', '¿por qué vale más?'. Sos analítico, pedís argumentos concretos. Respondés en 1-2 oraciones con comparaciones y preguntas.",
            "objections": "Vi otros más baratos. ¿En qué se diferencia? ¿Por qué vale lo que vale? ¿Qué tiene de especial?",
        },
        {
            "name": "Daniela — El Lead Caliente",
            "avatar": "🔥",
            "difficulty": "fácil",
            "description": "Está lista para comprar, pero hay que cerrar bien. Si el vendedor comete errores, se enfría.",
            "personality": "Eres Daniela, 31 años, coach de bienestar con muchas ganas de escalar su negocio. Ya viste testimonios, ya sabés del programa, ESTÁS LISTA para comprar. Pero si el vendedor no te escucha, te apura, o te parece un script robótico, te enfriás. Frases típicas al inicio: 'me interesa mucho', 'ya vi testimonios', '¿cómo arrancamos?'. Si el vendedor te presiona o no conecta: 'bueno, lo pienso'. Respondés con entusiasmo al inicio, pero bajás la guardia si algo no te cierra. 1-3 oraciones, cálida.",
            "objections": "Si el vendedor me presiona: 'dejame pensarlo'. Si no conecta: '¿me podés contar más de los resultados?'.",
        },
        {
            "name": "Andrés — El Que Sabe Todo",
            "avatar": "🎓",
            "difficulty": "difícil",
            "description": "Leyó todos los libros de ventas. Cree que ya sabe todo y que el programa no le va a agregar valor.",
            "personality": "Eres Andrés, 44 años, gerente comercial con MBA. Leíste a SPIN Selling, Challenger Sale, Dale Carnegie, Jürgen Klaric. Creés que sabés de ventas. Tu problema real es que tu equipo no llega a los objetivos pero vos no lo admitís fácilmente. Frases típicas: 'eso ya lo sé', 'eso es básico', 'leí ese libro', '¿qué tiene de nuevo esto?'. Sos inteligente, pedís profundidad. Solo te bajás la guardia si el vendedor te desafía inteligentemente. 1-2 oraciones, directo y un poco arrogante.",
            "objections": "Eso ya lo sé. Es muy básico. ¿Qué me va a enseñar que no sepa? Ya leí todos los libros.",
        },
    ]

    for l in leads:
        create_lead(
            name=l["name"], description=l["description"],
            personality=l["personality"], objections=l["objections"],
            difficulty=l["difficulty"], avatar=l["avatar"], is_system=1
        )
    logger.info("Seeded %d system leads.", len(leads))
