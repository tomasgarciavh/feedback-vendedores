import json
import logging
import os
import queue
import threading
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Argentina/Buenos_Aires")

from flask import Flask, Response, flash, jsonify, redirect, render_template, request, send_from_directory, session as flask_session, url_for

import config
import database
import processor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(_BASE_DIR, "uploads")
PHOTO_FOLDER = os.path.join(_BASE_DIR, "uploads", "photos")
ROLEPLAYS_FOLDER = os.path.join(_BASE_DIR, "uploads", "roleplays")
TESTIMONIALS_FOLDER = os.path.join(_BASE_DIR, "uploads", "testimonials")
ALLOWED_EXTENSIONS = {"mp4", "mov", "avi", "mkv", "webm", "m4v", "wmv", "flv", "3gp", "ts", "mts", "m2ts", "ogv", "f4v", "divx", "mpg", "mpeg", "mp2", "mpe", "mpv", "m2v"}
ALLOWED_PHOTO_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "gif"}

os.makedirs(ROLEPLAYS_FOLDER, exist_ok=True)
os.makedirs(TESTIMONIALS_FOLDER, exist_ok=True)

_processing_queue: queue.Queue = queue.Queue()
_lanzamiento_queue: queue.Queue = queue.Queue()

LANZAMIENTO_FOLDER = os.path.join(_BASE_DIR, "uploads", "lanzamiento")
os.makedirs(LANZAMIENTO_FOLDER, exist_ok=True)

LANZAMIENTO_EXTENSIONS = {"mp4", "mov", "avi", "mkv", "webm", "m4v", "wmv", "flv", "3gp",
                           "jpg", "jpeg", "png", "webp"}


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _allowed_photo(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_PHOTO_EXTENSIONS


def _allowed_lanzamiento(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in LANZAMIENTO_EXTENSIONS


def _worker():
    while True:
        item = _processing_queue.get()
        try:
            processor.process_uploaded_file(
                file_path=item["file_path"],
                vendor_name=item["vendor_name"],
                file_name=item["file_name"],
                file_id=item["file_id"],
            )
        except Exception as exc:
            logger.error("Worker error: %s", exc, exc_info=True)
        finally:
            _processing_queue.task_done()


def _lanzamiento_worker():
    import json as _json
    import re as _re
    while True:
        item = _lanzamiento_queue.get()
        file_id = item["file_id"]
        file_path = item["file_path"]
        vendor_name = item["vendor_name"]
        analysis_phase = item.get("analysis_phase")
        custom_instructions = item.get("custom_instructions")
        try:
            import gemini_analyzer
            raw_feedback = gemini_analyzer.analyze_lanzamiento(
                file_path=file_path,
                vendor_name=vendor_name,
                analysis_phase=analysis_phase,
                custom_instructions=custom_instructions,
            )
            # Extract scores JSON block
            score, section_scores, clean_feedback = None, None, raw_feedback
            pattern = r"```json_scores\s*(\{.*?\})\s*```"
            match = _re.search(pattern, raw_feedback, _re.DOTALL)
            if match:
                raw_json = match.group(1)
                clean_feedback = raw_feedback[:match.start()].rstrip()
                clean_feedback = _re.sub(r"\n+---\n+### SCORES\s*$", "", clean_feedback).rstrip()
                try:
                    data = _json.loads(raw_json)
                    score = float(data.get("score_general", 0)) or None
                    section_keys = ["relacion", "descubrimiento", "siembra", "recomendacion",
                                    "objeciones", "epp_formula", "comunicacion", "mentalidad"]
                    sections = {k: float(data.get(k, 0)) for k in section_keys}
                    section_scores = _json.dumps(sections)
                except Exception:
                    pass
            database.lanzamiento_mark_done(file_id, clean_feedback, score=score, section_scores=section_scores)
            logger.info("Lanzamiento feedback done: %s", file_id)
        except Exception as exc:
            logger.error("Lanzamiento worker error: %s", exc, exc_info=True)
            database.lanzamiento_mark_error(file_id, str(exc))
        finally:
            # Delete file after processing — feedback is stored in DB
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
                    logger.info("Deleted processed lanzamiento file: %s", file_path)
            except Exception as del_exc:
                logger.warning("Could not delete file %s: %s", file_path, del_exc)
            _lanzamiento_queue.task_done()


app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-me-in-production")
app.config["MAX_CONTENT_LENGTH"] = config.MAX_UPLOAD_MB * 1024 * 1024
# Stream large uploads directly to disk — evita cargarlos en memoria RAM
app.config["MAX_FORM_MEMORY_SIZE"] = 1 * 1024 * 1024  # solo 1 MB en RAM, el resto va a disco


@app.template_filter("fromjson")
def _fromjson(s):
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}


@app.template_filter("fmt_date")
def _fmt_date(s):
    """Convert YYYY-MM-DD or YYYY-MM-DDTHH:MM... to DD/MM/YYYY or DD/MM/YYYY HH:MM."""
    if not s:
        return ""
    s = str(s)
    try:
        if "T" in s or (len(s) > 10 and s[10] == " "):
            sep = "T" if "T" in s else " "
            date_part, time_part = s.split(sep, 1)
            y, m, d = date_part.split("-")
            return f"{d}/{m}/{y} {time_part[:5]}"
        y, m, d = s[:10].split("-")
        return f"{d}/{m}/{y}"
    except Exception:
        return s


@app.context_processor
def inject_current_vendor():
    vid = flask_session.get("vendor_id")
    if vid:
        vendor = database.get_vendor_by_id(vid)
        return {"current_vendor": vendor}
    return {"current_vendor": None}


@app.after_request
def add_no_cache(response):
    # Skip no-cache for images so the browser can display them
    if response.content_type and response.content_type.startswith("image/"):
        return response
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ── Health check ───────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return "ok", 200


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    redir = _require_vendor()
    if redir: return redir
    records = database.get_recent_records(limit=200)
    vendors = database.get_vendors()
    missing_configs = config.get_missing_configs()
    pending_count = database.count_pending()
    queue_size = _processing_queue.qsize()

    # Useful metrics
    done_records = [r for r in records if r["status"] == "done" and r["score"]]
    all_scores = [r["score"] for r in done_records]
    avg_score = round(sum(all_scores) / len(all_scores), 1) if all_scores else None

    vendor_session_counts = {}
    for r in done_records:
        vn = r["vendor_name"]
        vendor_session_counts[vn] = vendor_session_counts.get(vn, 0) + 1
    avg_roleplays = round(sum(vendor_session_counts.values()) / len(vendor_session_counts), 1) if vendor_session_counts else None

    best_vendor = max(
        {vn: sum(r["score"] for r in done_records if r["vendor_name"] == vn) / cnt
         for vn, cnt in vendor_session_counts.items()}.items(),
        key=lambda x: x[1], default=(None, None)
    )

    return render_template(
        "index.html",
        records=records,
        vendors=vendors,
        missing_configs=missing_configs,
        pending_count=pending_count,
        queue_size=queue_size,
        avg_score=avg_score,
        avg_roleplays=avg_roleplays,
        best_vendor_name=best_vendor[0],
        best_vendor_score=round(best_vendor[1], 1) if best_vendor[1] else None,
        total_roleplays=len(done_records),
    )


# ── Upload ─────────────────────────────────────────────────────────────────────

@app.route("/upload", methods=["POST"])
def upload():
    vendor_name = (request.form.get("vendor_name") or "").strip()
    if not vendor_name:
        flash("Seleccioná un vendedor.", "danger")
        return redirect(url_for("index"))

    if "video" not in request.files or request.files["video"].filename == "":
        flash("Seleccioná un archivo de video.", "danger")
        return redirect(url_for("index"))

    file = request.files["video"]
    if not _allowed_file(file.filename):
        flash(f"Formato no soportado. Usá: {', '.join(sorted(ALLOWED_EXTENSIONS))}", "danger")
        return redirect(url_for("index"))

    ext = file.filename.rsplit(".", 1)[1].lower()
    file_id = str(uuid.uuid4())
    saved_name = f"{file_id}.{ext}"
    file_path = os.path.join(UPLOAD_FOLDER, saved_name)

    # Guardar en chunks para no cargar el archivo entero en RAM
    chunk_size = 8 * 1024 * 1024  # 8 MB por chunk
    try:
        with open(file_path, "wb") as f:
            while True:
                chunk = file.stream.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as exc:
        if os.path.exists(file_path):
            os.unlink(file_path)
        flash(f"Error al guardar el archivo: {exc}", "danger")
        return redirect(url_for("index"))

    database.mark_processing(file_id, file.filename, vendor_name, None)
    _processing_queue.put({
        "file_id": file_id,
        "file_path": file_path,
        "vendor_name": vendor_name,
        "file_name": file.filename,
    })

    flash(f"Video de {vendor_name} en cola. El feedback estará listo en unos minutos.", "success")
    return redirect(url_for("index"))


# ── Upload from Drive link ─────────────────────────────────────────────────────

def _extract_drive_file_id(url: str):
    """Extract Google Drive file ID from various share URL formats."""
    import re
    patterns = [
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"/open\?id=([a-zA-Z0-9_-]+)",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def _download_from_drive(file_id_drive: str, dest_path: str) -> str:
    """Download a Google Drive file, handling large-file confirmation pages."""
    import re
    import requests as _req

    sess = _req.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})

    base_url = "https://drive.google.com/uc"
    params = {"export": "download", "id": file_id_drive}

    resp = sess.get(base_url, params=params, stream=True, timeout=60)
    resp.raise_for_status()

    ctype = resp.headers.get("Content-Type", "")
    if "text/html" in ctype:
        html = resp.text
        # Extract confirm token (old and new style)
        confirm = None
        m = re.search(r'name="confirm"\s+value="([^"]+)"', html)
        if m:
            confirm = m.group(1)
        else:
            m = re.search(r'confirm=([0-9A-Za-z_\-]+)', html)
            if m:
                confirm = m.group(1)

        if confirm:
            params["confirm"] = confirm
            m2 = re.search(r'name="uuid"\s+value="([^"]+)"', html)
            if m2:
                params["uuid"] = m2.group(1)
        else:
            params["confirm"] = "t"

        resp = sess.get(base_url, params=params, stream=True, timeout=120)
        resp.raise_for_status()

    total = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)
                total += len(chunk)

    if total < 2048:
        raise RuntimeError(
            "El archivo descargado es demasiado pequeño. "
            "Verificá que esté compartido con 'Cualquiera con el link'."
        )
    return dest_path


@app.route("/upload_drive", methods=["POST"])
def upload_drive():
    vendor_name = (request.form.get("vendor_name") or "").strip()
    drive_url = (request.form.get("drive_url") or "").strip()

    # If no vendor selected but user is logged in, use their own name
    if not vendor_name:
        vid = flask_session.get("vendor_id")
        if vid:
            v = database.get_vendor_by_id(vid)
            if v:
                vendor_name = v["name"]

    if not vendor_name:
        return jsonify({"ok": False, "error": "Seleccioná un vendedor."}), 400
    if not drive_url:
        return jsonify({"ok": False, "error": "Pegá el link de Google Drive."}), 400

    file_id_drive = _extract_drive_file_id(drive_url)
    if not file_id_drive:
        return jsonify({"ok": False, "error": "No se pudo extraer el ID del link. Asegurate de compartir el archivo con 'Cualquiera con el link'."}), 400

    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.mp4")

    try:
        _download_from_drive(file_id_drive, file_path)

        # Detect real extension from downloaded file
        import subprocess
        orig_name = f"video_drive_{file_id_drive[:8]}.mp4"
        try:
            result = subprocess.run(
                ["file", "--mime-type", "-b", file_path],
                capture_output=True, text=True, timeout=10
            )
            mime = result.stdout.strip()
            ext_map = {
                "video/mp4": "mp4", "video/quicktime": "mov",
                "video/x-msvideo": "avi", "video/x-matroska": "mkv",
                "video/webm": "webm", "video/3gpp": "3gp",
            }
            detected_ext = ext_map.get(mime)
            if detected_ext and detected_ext != "mp4":
                new_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.{detected_ext}")
                os.rename(file_path, new_path)
                file_path = new_path
                orig_name = f"video_drive_{file_id_drive[:8]}.{detected_ext}"
        except Exception:
            pass

    except Exception as exc:
        if os.path.exists(file_path):
            os.unlink(file_path)
        return jsonify({"ok": False, "error": f"Error al descargar el archivo de Drive: {exc}"}), 500

    database.mark_processing(file_id, orig_name, vendor_name, None)
    _processing_queue.put({
        "file_id": file_id,
        "file_path": file_path,
        "vendor_name": vendor_name,
        "file_name": orig_name,
    })

    return jsonify({"ok": True, "message": f"Video de {vendor_name} en cola. El feedback estará listo en unos minutos."})


# ── Vendors list ───────────────────────────────────────────────────────────────

@app.route("/vendors", methods=["GET", "POST"])
def vendors():
    redir = _require_vendor()
    if redir: return redir
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "sin-email@local").strip() or "sin-email@local"
        if not name:
            flash("El nombre del vendedor es obligatorio.", "danger")
        else:
            try:
                database.upsert_vendor(name, email)
                flash(f"Vendedor '{name}' guardado correctamente.", "success")
            except Exception as exc:
                logger.error("Error saving vendor: %s", exc, exc_info=True)
                flash(f"Error al guardar: {exc}", "danger")
        return redirect(url_for("vendors"))

    vendors_list = database.get_vendors()
    all_records = database.get_recent_records(limit=500)

    # Build per-vendor stats
    vendor_stats = {}
    for v in vendors_list:
        recs = [r for r in all_records
                if r["vendor_name"].strip().lower() == v["name"].strip().lower()
                and r["status"] == "done" and r["score"]]
        scores = [r["score"] for r in recs]
        section_sums = {}
        section_counts = {}
        for r in recs:
            if r["section_scores"]:
                try:
                    sec = json.loads(r["section_scores"])
                    for k, val in sec.items():
                        section_sums[k] = section_sums.get(k, 0) + float(val)
                        section_counts[k] = section_counts.get(k, 0) + 1
                except Exception:
                    pass
        section_avgs = {k: round(section_sums[k] / section_counts[k], 1)
                        for k in section_sums if section_counts[k]}

        trend = None
        if len(scores) >= 2:
            trend = "up" if scores[-1] > scores[-2] else ("down" if scores[-1] < scores[-2] else "equal")

        vendor_stats[v["id"]] = {
            "sessions": len(recs),
            "avg_score": round(sum(scores) / len(scores), 1) if scores else None,
            "best_score": max(scores) if scores else None,
            "last_score": scores[-1] if scores else None,
            "trend": trend,
            "section_avgs": section_avgs,
        }

    return render_template("vendors.html", vendors=vendors_list, vendor_stats=vendor_stats)


@app.route("/records/<file_id>", methods=["DELETE"])
def delete_record(file_id: str):
    try:
        database.delete_record(file_id)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/vendors/<int:vendor_id>", methods=["DELETE"])
def delete_vendor(vendor_id: int):
    try:
        database.delete_vendor(vendor_id)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Vendor profile ─────────────────────────────────────────────────────────────

@app.route("/vendors/<int:vendor_id>/profile")
def vendor_profile(vendor_id: int):
    redir = _require_vendor()
    if redir: return redir
    vendor = database.get_vendor_by_id(vendor_id)
    if not vendor:
        flash("Vendedor no encontrado.", "danger")
        return redirect(url_for("vendors"))

    # Parse metrics JSON
    try:
        vendor_metrics = json.loads(vendor.get("metrics") or "{}")
    except Exception:
        vendor_metrics = {}

    records = database.get_vendor_records(vendor["name"])

    # Build chart data
    labels = []
    scores_line = []
    section_radar_sums = {
        "diagnostico_desapego": 0, "descubrimiento_acuerdos": 0, "empatia_escucha": 0,
        "ingenieria_preguntas": 0, "gestion_creencias": 0, "storytelling": 0,
        "pitch_personalizado": 0, "mentalidad": 0,
    }
    scored_count = 0

    for r in records:
        raw_date = (r["processed_at"] or "")[:10]
        date_label = _fmt_date(raw_date) if raw_date else ""
        labels.append(date_label)
        scores_line.append(r["score"] if r["score"] else None)

        if r["section_scores"]:
            try:
                sec = json.loads(r["section_scores"])
                for k in section_radar_sums:
                    section_radar_sums[k] += sec.get(k, 0)
                scored_count += 1
            except Exception:
                pass

    # Average section scores for radar chart
    radar_data = []
    for k in ["diagnostico_desapego", "descubrimiento_acuerdos", "empatia_escucha",
              "ingenieria_preguntas", "gestion_creencias", "storytelling",
              "pitch_personalizado", "mentalidad"]:
        val = round(section_radar_sums[k] / scored_count, 1) if scored_count else 0
        radar_data.append(val)

    # KPIs
    valid_scores = [s for s in scores_line if s is not None]
    avg_score = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else None
    best_score = max(valid_scores) if valid_scores else None
    total_sessions = len(records)

    # Trend: compare last 3 vs previous 3
    trend = None
    if len(valid_scores) >= 2:
        last = valid_scores[-1]
        prev = valid_scores[-2]
        trend = "up" if last > prev else ("down" if last < prev else "equal")

    # Last feedback for FODA + action plan display
    last_feedback = None
    for r in reversed(records):
        if r.get("feedback_text"):
            last_feedback = r["feedback_text"]
            break

    return render_template(
        "vendor_profile.html",
        vendor=vendor,
        vendor_metrics=vendor_metrics,
        records=records,
        chart_labels=json.dumps(labels),
        chart_scores=json.dumps(scores_line),
        radar_data=json.dumps(radar_data),
        avg_score=avg_score,
        best_score=best_score,
        total_sessions=total_sessions,
        scored_count=scored_count,
        valid_scores=valid_scores,
        trend=trend,
        last_feedback=last_feedback,
    )


@app.route("/vendors/<int:vendor_id>/info", methods=["POST"])
def update_vendor_info(vendor_id: int):
    try:
        data = request.get_json()
        import json as _json
        metrics_raw = data.get("metrics", {})
        database.update_vendor_info(
            vendor_id,
            role=data.get("role", ""),
            phone=data.get("phone", ""),
            bio=data.get("bio", ""),
            objectives=data.get("objectives", ""),
            achievements=data.get("achievements", ""),
            results=data.get("results", ""),
            experience=data.get("experience", ""),
            status=data.get("status", ""),
            joined_program=data.get("joined_program", ""),
            metrics=_json.dumps(metrics_raw) if metrics_raw else None,
        )
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/vendors/<int:vendor_id>/photo", methods=["POST"])
def upload_photo(vendor_id: int):
    vendor = database.get_vendor_by_id(vendor_id)
    if not vendor:
        return jsonify({"ok": False, "error": "Vendedor no encontrado"}), 404

    if "photo" not in request.files or request.files["photo"].filename == "":
        return jsonify({"ok": False, "error": "No se recibió ninguna foto"}), 400

    file = request.files["photo"]
    if not _allowed_photo(file.filename):
        return jsonify({"ok": False, "error": "Formato no soportado. Usá JPG, PNG o WEBP"}), 400

    ext = file.filename.rsplit(".", 1)[1].lower()
    filename = f"vendor_{vendor_id}.{ext}"
    file_path = os.path.join(PHOTO_FOLDER, filename)
    file.save(file_path)

    database.update_vendor_photo(vendor_id, filename)
    return jsonify({"ok": True, "filename": filename})


@app.route("/uploads/photos/<filename>")
def vendor_photo(filename: str):
    filepath = os.path.join(PHOTO_FOLDER, filename)
    if not os.path.isfile(filepath):
        from flask import abort
        abort(404)
    return send_from_directory(PHOTO_FOLDER, filename)


def _stream_video(filepath: str) -> Response:
    """Stream a video file with Range support for HTML5 video players."""
    file_size = os.path.getsize(filepath)
    ext = filepath.rsplit(".", 1)[-1].lower()
    mime = {
        "mp4": "video/mp4", "mov": "video/quicktime", "avi": "video/x-msvideo",
        "mkv": "video/x-matroska", "webm": "video/webm", "m4v": "video/mp4",
        "wmv": "video/x-ms-wmv", "flv": "video/x-flv", "3gp": "video/3gpp",
    }.get(ext, "video/mp4")

    range_header = request.headers.get("Range")
    if range_header:
        byte1, byte2 = 0, None
        m = __import__("re").search(r"bytes=(\d+)-(\d*)", range_header)
        if m:
            byte1 = int(m.group(1))
            byte2 = int(m.group(2)) if m.group(2) else file_size - 1
        length = byte2 - byte1 + 1
        with open(filepath, "rb") as f:
            f.seek(byte1)
            data = f.read(length)
        resp = Response(data, 206, mimetype=mime, direct_passthrough=True)
        resp.headers["Content-Range"] = f"bytes {byte1}-{byte2}/{file_size}"
        resp.headers["Accept-Ranges"] = "bytes"
        resp.headers["Content-Length"] = str(length)
        return resp

    resp = Response(open(filepath, "rb").read(), 200, mimetype=mime)
    resp.headers["Accept-Ranges"] = "bytes"
    resp.headers["Content-Length"] = str(file_size)
    return resp


@app.route("/uploads/video/<file_id>")
def serve_video(file_id: str):
    """Stream roleplay video with Range support."""
    import glob as _glob
    for folder in (ROLEPLAYS_FOLDER, UPLOAD_FOLDER):
        matches = [m for m in _glob.glob(os.path.join(folder, f"{file_id}.*"))
                   if os.path.isfile(m)]
        if matches:
            return _stream_video(matches[0])
    return "", 404


@app.route("/vendors/<int:vendor_id>/testimonial", methods=["POST"])
def upload_testimonial(vendor_id: int):
    """Upload a testimonial video for a vendor."""
    vendor = database.get_vendor_by_id(vendor_id)
    if not vendor:
        return jsonify({"ok": False, "error": "Vendedor no encontrado"}), 404

    if "video" not in request.files:
        return jsonify({"ok": False, "error": "No se recibió ningún archivo"}), 400

    file = request.files["video"]
    if not file.filename:
        return jsonify({"ok": False, "error": "Archivo sin nombre"}), 400

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({"ok": False, "error": f"Formato no soportado. Usá: {', '.join(sorted(ALLOWED_EXTENSIONS))}"}), 400

    # Remove old testimonial if exists
    old_path = vendor.get("testimonial_video")
    if old_path:
        old_full = os.path.join(TESTIMONIALS_FOLDER, old_path)
        if os.path.exists(old_full):
            os.unlink(old_full)

    filename = f"vendor_{vendor_id}.{ext}"
    file.save(os.path.join(TESTIMONIALS_FOLDER, filename))
    database.update_vendor_testimonial(vendor_id, filename)
    return jsonify({"ok": True, "filename": filename})


@app.route("/uploads/testimonials/<filename>")
def serve_testimonial(filename: str):
    filepath = os.path.join(TESTIMONIALS_FOLDER, filename)
    if not os.path.isfile(filepath):
        return "", 404
    return _stream_video(filepath)


# ── Analytics ─────────────────────────────────────────────────────────────────

@app.route("/analytics")
def analytics():
    redir = _require_vendor()
    if redir: return redir
    data = database.get_analytics_data()
    return render_template("analytics.html", **data)


@app.route("/formacion")
def formacion_vh():
    redir = _require_vendor()
    if redir: return redir
    return render_template("formacion_vh.html")


# ── Roleplay Chat ──────────────────────────────────────────────────────────────

def _vendor_session():
    """Returns the logged-in vendor dict from Flask session, or None."""
    vid = flask_session.get("vendor_id") or flask_session.get("chat_vendor_id")
    if not vid:
        return None
    return database.get_vendor_by_id(vid)


def _require_vendor():
    """Redirects to vendor login if not authenticated."""
    if not flask_session.get("vendor_id"):
        return redirect(url_for("vendor_login", next=request.path))
    return None


def _require_producer():
    """Returns redirect to producer login if not authenticated, else None."""
    if not flask_session.get("producer_auth"):
        return redirect(url_for("productor_login", next=request.path))
    return None


# ── Vendor login / logout ───────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def vendor_login():
    if flask_session.get("vendor_id"):
        return redirect(url_for("index"))
    error = None
    first_time = False
    prefill_email = ""
    prefill_name = ""
    preserved_vendor_id = ""
    show_name_selector = False
    all_vendors = []

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        confirm = (request.form.get("confirm") or "").strip()
        vendor_id_pick = request.form.get("vendor_id_pick", "").strip()
        prefill_email = email

        # If vendor identified themselves via name picker → look up by ID
        if vendor_id_pick:
            vendor = database.get_vendor_by_id(int(vendor_id_pick))
            # Update email in DB so future logins work with their real email
            if vendor and email and email != (vendor.get("email") or "").lower():
                database.update_vendor_email(int(vendor_id_pick), email)
                vendor["email"] = email
        else:
            vendor = database.get_vendor_by_email(email)
            if not vendor:
                vendor = database.get_vendor_by_name(email)

        if not vendor_id_pick and not vendor:
            # Email not found — show name selector so vendor can identify themselves
            show_name_selector = True
            all_vendors = database.get_all_vendors()
        elif vendor:
            has_pin = bool(vendor.get("pin"))
            if not has_pin:
                if not password:
                    first_time = True
                    prefill_name = vendor["name"]
                    preserved_vendor_id = str(vendor["id"])
                elif len(password) < 4:
                    error = "La contraseña debe tener al menos 4 caracteres."
                    first_time = True
                    preserved_vendor_id = str(vendor["id"])
                elif password != confirm:
                    error = "Las contraseñas no coinciden."
                    first_time = True
                    preserved_vendor_id = str(vendor["id"])
                else:
                    database.update_vendor_pin(vendor["id"], password)
                    flask_session["vendor_id"] = vendor["id"]
                    flask_session["chat_vendor_id"] = vendor["id"]
                    next_url = request.args.get("next") or url_for("index")
                    return redirect(next_url)
            else:
                if not password:
                    error = "Ingresá tu contraseña."
                    prefill_name = vendor["name"]
                    preserved_vendor_id = str(vendor["id"])
                elif vendor.get("pin") != password:
                    error = "Contraseña incorrecta."
                    prefill_name = vendor["name"]
                    preserved_vendor_id = str(vendor["id"])
                else:
                    flask_session["vendor_id"] = vendor["id"]
                    flask_session["chat_vendor_id"] = vendor["id"]
                    next_url = request.args.get("next") or url_for("index")
                    return redirect(next_url)

    return render_template("vendor_login.html", error=error, first_time=first_time,
                           prefill_email=prefill_email,
                           prefill_name=prefill_name,
                           preserved_vendor_id=preserved_vendor_id,
                           show_name_selector=show_name_selector,
                           all_vendors=all_vendors)


@app.route("/logout")
def vendor_logout():
    flask_session.pop("vendor_id", None)
    flask_session.pop("chat_vendor_id", None)
    return redirect(url_for("vendor_login"))


# ── Producer login ─────────────────────────────────────────────────────────────

@app.route("/productor/login", methods=["GET", "POST"])
def productor_login():
    if flask_session.get("producer_auth"):
        return redirect(url_for("ventas"))
    error = None
    if request.method == "POST":
        pwd = (request.form.get("password") or "").strip()
        if pwd == config.PRODUCER_PASSWORD:
            flask_session["producer_auth"] = True
            next_url = request.args.get("next") or url_for("ventas")
            return redirect(next_url)
        error = "Contraseña incorrecta."
    return render_template("productor_login.html", error=error)


@app.route("/productor/logout")
def productor_logout():
    flask_session.pop("producer_auth", None)
    return redirect(url_for("index"))


@app.route("/admin/vendedores", methods=["GET", "POST"])
def admin_vendedores():
    """Standalone vendor management page with its own login — no nav required."""
    SESSION_KEY = "admin_vendedores_auth"
    auth = flask_session.get(SESSION_KEY) or flask_session.get("producer_auth")
    login_error = None

    if request.method == "POST":
        action = request.form.get("action")
        if action == "login":
            pwd = (request.form.get("password") or "").strip()
            if pwd == config.PRODUCER_PASSWORD:
                flask_session[SESSION_KEY] = True
                auth = True
            else:
                login_error = "Contraseña incorrecta."
        elif action == "logout":
            flask_session.pop(SESSION_KEY, None)
            return redirect(url_for("admin_vendedores"))
        elif auth:
            if action == "reset_all":
                count = database.reset_all_vendor_pins()
                flash(f"✅ Se resetearon las contraseñas de {count} vendedores.", "success")
                return redirect(url_for("admin_vendedores"))
            elif action == "reset_pin":
                vid = request.form.get("vendor_id", type=int)
                if vid:
                    database.update_vendor_pin(vid, None)
                    flash("✅ Contraseña reseteada.", "success")
                return redirect(url_for("admin_vendedores"))

    vendors = database.get_all_vendors_with_pins() if auth else []
    return render_template("admin_vendedores.html", auth=auth, vendors=vendors, login_error=login_error)


@app.route("/admin/vendedores/update-field", methods=["POST"])
def admin_vendedores_update_field():
    if not (flask_session.get("admin_vendedores_auth") or flask_session.get("producer_auth")):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    vid = data.get("vendor_id")
    field = data.get("field")
    value = (data.get("value") or "").strip()
    if not vid or field not in ("name", "email") or not value:
        return jsonify({"ok": False, "error": "Datos inválidos"}), 400
    if field == "email":
        if "@" not in value:
            return jsonify({"ok": False, "error": "Email inválido"}), 400
        database.update_vendor_email(int(vid), value)
    else:
        database.update_vendor_name(int(vid), value)
    return jsonify({"ok": True})


@app.route("/admin/vendedores/add", methods=["POST"])
def admin_vendedores_add():
    if not (flask_session.get("admin_vendedores_auth") or flask_session.get("producer_auth")):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    if not name or not email or "@" not in email:
        return jsonify({"ok": False, "error": "Nombre y email requeridos"}), 400
    vid = database.add_vendor(name, email)
    return jsonify({"ok": True, "vendor_id": vid})


@app.route("/admin/vendedores/run-seed", methods=["POST"])
def admin_run_seed():
    if not (flask_session.get("admin_vendedores_auth") or flask_session.get("producer_auth")):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    result = database.run_seed_report()
    return jsonify({"ok": True, "result": result})


@app.route("/admin/vendedores/delete", methods=["POST"])
def admin_vendedores_delete():
    if not (flask_session.get("admin_vendedores_auth") or flask_session.get("producer_auth")):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    vid = data.get("vendor_id")
    if not vid:
        return jsonify({"ok": False, "error": "Falta vendor_id"}), 400
    database.delete_vendor(int(vid))
    return jsonify({"ok": True})


@app.route("/productor/reset-pins", methods=["POST"])
def productor_reset_pins():
    redir = _require_producer()
    if redir:
        return redir
    count = database.reset_all_vendor_pins()
    flash(f"✅ Se resetearon las contraseñas de {count} vendedores. Todos deben crear una nueva al próximo ingreso.", "success")
    return redirect(url_for("index"))


@app.route("/productor/vendors")
def productor_vendors():
    redir = _require_producer()
    if redir: return redir
    vendors = database.get_all_vendors_with_pins()
    return render_template("productor_vendors.html", vendors=vendors)


@app.route("/productor/update-vendor-email", methods=["POST"])
def productor_update_vendor_email():
    redir = _require_producer()
    if redir: return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    vendor_id = data.get("vendor_id")
    email = (data.get("email") or "").strip().lower()
    if not vendor_id or not email or "@" not in email:
        return jsonify({"ok": False, "error": "Datos inválidos"}), 400
    database.update_vendor_email(int(vendor_id), email)
    return jsonify({"ok": True})


@app.route("/productor/update-vendor-field", methods=["POST"])
def productor_update_vendor_field():
    redir = _require_producer()
    if redir: return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    vendor_id = data.get("vendor_id")
    field = data.get("field")
    value = (data.get("value") or "").strip()
    if not vendor_id or field not in ("name", "email") or not value:
        return jsonify({"ok": False, "error": "Datos inválidos"}), 400
    if field == "email":
        if "@" not in value:
            return jsonify({"ok": False, "error": "Email inválido"}), 400
        database.update_vendor_email(int(vendor_id), value)
    else:
        database.update_vendor_name(int(vendor_id), value)
    return jsonify({"ok": True})


@app.route("/productor/add-vendor", methods=["POST"])
def productor_add_vendor():
    redir = _require_producer()
    if redir: return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    if not name or not email or "@" not in email:
        return jsonify({"ok": False, "error": "Nombre y email son obligatorios"}), 400
    vid = database.add_vendor(name, email)
    return jsonify({"ok": True, "vendor_id": vid})


@app.route("/productor/delete-vendor", methods=["POST"])
def productor_delete_vendor():
    redir = _require_producer()
    if redir: return jsonify({"ok": False, "error": "No autorizado"}), 403
    data = request.get_json()
    vendor_id = data.get("vendor_id")
    if not vendor_id:
        return jsonify({"ok": False, "error": "Falta vendor_id"}), 400
    database.delete_vendor(int(vendor_id))
    return jsonify({"ok": True})


@app.route("/productor/reset-pin/<int:vendor_id>", methods=["POST"])
def productor_reset_single_pin(vendor_id):
    redir = _require_producer()
    if redir: return redir
    database.update_vendor_pin(vendor_id, None)
    flash("✅ Contraseña reseteada. El vendedor deberá crear una nueva al próximo ingreso.", "success")
    return redirect(url_for("productor_vendors"))


@app.route("/chat")
def chat():
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("vendor_login", next=request.path))
    system_leads = database.get_system_leads()
    my_leads = database.get_vendor_leads(vendor["id"])
    sessions = database.get_vendor_sessions(vendor["id"])
    gamif = database.get_gamification(vendor["id"])
    import json as _json
    gamif["badges"] = _json.loads(gamif.get("badges_json") or "[]")
    gamif["level"] = database.get_level_info(gamif.get("xp") or 0)
    gamif["all_badges"] = database.BADGES
    return render_template("chat.html", vendor=vendor,
                           system_leads=system_leads, my_leads=my_leads,
                           sessions=sessions, gamif=gamif)


@app.route("/chat/gamification")
def chat_gamification_api():
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False}), 401
    import json as _json
    gamif = database.get_gamification(vendor["id"])
    gamif["badges"] = _json.loads(gamif.get("badges_json") or "[]")
    gamif["level"] = database.get_level_info(gamif.get("xp") or 0)
    return jsonify({"ok": True, **gamif})


@app.route("/chat/login", methods=["GET", "POST"])
def chat_login():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        vendor = database.get_vendor_by_name(name)
        if vendor:
            flask_session["chat_vendor_id"] = vendor["id"]
            return redirect(url_for("chat"))
        flash("Nombre no encontrado.", "danger")
    vendors = database.get_vendors()
    return render_template("chat_login.html", vendors=vendors)


@app.route("/chat/logout")
def chat_logout():
    flask_session.pop("chat_vendor_id", None)
    return redirect(url_for("vendor_login", next=request.path))


@app.route("/chat/leads", methods=["POST"])
def chat_create_lead():
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    data = request.get_json()
    lead_id = database.create_lead(
        name=data.get("name", "Lead personalizado"),
        description=data.get("description", ""),
        personality=data.get("personality", ""),
        objections=data.get("objections", ""),
        difficulty=data.get("difficulty", "medio"),
        avatar=data.get("avatar", "👤"),
        vendor_id=vendor["id"],
    )
    lead = database.get_lead_by_id(lead_id)
    return jsonify({"ok": True, "lead": lead})


@app.route("/chat/leads/<int:lead_id>", methods=["DELETE"])
def chat_delete_lead(lead_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False}), 401
    database.delete_lead(lead_id, vendor["id"])
    return jsonify({"ok": True})


@app.route("/chat/session/start", methods=["POST"])
def chat_start_session():
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    data = request.get_json()
    lead_id = data.get("lead_id")
    lead = database.get_lead_by_id(lead_id)
    if not lead:
        return jsonify({"ok": False, "error": "Lead no encontrado"}), 404
    session_id = database.create_roleplay_session(vendor["id"], lead_id)
    return jsonify({"ok": True, "session_id": session_id})


@app.route("/chat/session/<int:session_id>/message", methods=["POST"])
def chat_message(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    sess = database.get_session(session_id)
    if not sess or sess["vendor_id"] != vendor["id"]:
        return jsonify({"ok": False, "error": "Sesión no válida"}), 403
    if sess["status"] != "active":
        return jsonify({"ok": False, "error": "Sesión cerrada"}), 400

    data = request.get_json()
    user_text = (data.get("text") or "").strip()
    if not user_text:
        return jsonify({"ok": False, "error": "Mensaje vacío"}), 400

    lead = database.get_lead_by_id(sess["lead_id"])

    import google.generativeai as genai
    import json as _json

    messages = _json.loads(sess["messages_json"] or "[]")
    messages.append({"role": "vendor", "text": user_text})

    # Build prompt for Gemini
    history_text = "\n".join(
        f"{'Vendedor' if m['role']=='vendor' else lead['name']}: {m['text']}"
        for m in messages
    )

    # Last vendor message for coaching reference
    last_vendor_msg = user_text

    system_prompt = f"""Sos coach de ventas y actor simultáneo. Tu trabajo tiene DOS partes.

=== PARTE 1: RESPUESTA DEL LEAD ===
Interpretás el personaje: {lead['name']}
Descripción: {lead['description']}
Personalidad y comportamiento: {lead['personality']}
Objeciones típicas: {lead['objections']}

REGLAS DEL PERSONAJE:
- Respondé SOLO como {lead['name']}, nunca rompas el personaje.
- Tus respuestas son cortas: 1 a 3 oraciones máximo, como mensajes de WhatsApp.
- Hablá en español argentino informal.
- No digas que sos una IA ni que esto es un roleplay.
- Si el vendedor comete errores graves (presión, falta de escucha, promesas vacías), reaccioná enfriándote o poniendo más resistencia.
- Si el vendedor hace un buen trabajo (escucha activa, empatía, preguntas buenas), comenzá a abrirte un poco más.

=== PARTE 2: COACHING INMEDIATO ===
Analizá el ÚLTIMO mensaje del vendedor: "{last_vendor_msg}"

Sé ESTRICTO y honesto. No regales puntos. Si el mensaje fue mediocre, decilo. Si fue un error, marcalo claramente.
Cada punto debe ser concreto, específico y accionable — nada de frases genéricas.

=== FORMATO DE RESPUESTA (obligatorio, exacto) ===

LEAD:
[tu respuesta como {lead['name']}]

COACHING:
✅ **Bien:** [qué hizo bien — citá exactamente qué parte del mensaje funcionó, o escribí "nada destacable" si no hubo nada bueno]
⚠️ **A mejorar:** [qué error cometió o qué le faltó — sé específico, no suavices]
💡 **Alternativa:** [un mensaje listo para copiar y pegar que sería mejor]
🎯 **Por qué:** [el impacto concreto de ese error en la venta — qué pierde si lo sigue haciendo]

Historial de la conversación:
{history_text}"""

    try:
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(config.GEMINI_MODEL)
        response = model.generate_content(system_prompt)
        raw = response.text.strip()
    except Exception as e:
        logger.error("Gemini chat error: %s", e)
        return jsonify({"ok": False, "error": "Error al generar respuesta"}), 500

    # Parse LEAD and COACHING sections
    coaching_tip = None
    if "COACHING:" in raw:
        parts = raw.split("COACHING:", 1)
        lead_part = parts[0]
        coaching_tip = parts[1].strip()
    else:
        lead_part = raw

    # Extract lead reply
    if "LEAD:" in lead_part:
        lead_reply = lead_part.split("LEAD:", 1)[1].strip()
    else:
        lead_reply = lead_part.strip()

    messages.append({"role": "lead", "text": lead_reply})
    database.update_session_messages(session_id, _json.dumps(messages))

    return jsonify({"ok": True, "reply": lead_reply, "coaching_tip": coaching_tip, "messages": messages})


@app.route("/chat/session/<int:session_id>/end", methods=["POST"])
def chat_end_session(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    sess = database.get_session(session_id)
    if not sess or sess["vendor_id"] != vendor["id"]:
        return jsonify({"ok": False, "error": "Sesión no válida"}), 403
    if sess["status"] != "active":
        return jsonify({"ok": False, "error": "Sesión ya cerrada"}), 400

    lead = database.get_lead_by_id(sess["lead_id"])

    import google.generativeai as genai
    import json as _json

    messages = _json.loads(sess["messages_json"] or "[]")
    if len(messages) < 2:
        return jsonify({"ok": False, "error": "Conversación demasiado corta para evaluar"}), 400

    conversation_text = "\n".join(
        f"{'VENDEDOR' if m['role']=='vendor' else 'LEAD'}: {m['text']}"
        for m in messages
    )

    feedback_prompt = f"""Sos un coach experto y EXIGENTE en ventas de la metodología Vendedores Humanos (VH).
Acabás de observar este roleplay de práctica: el VENDEDOR practicó una conversación por chat con el lead "{lead['name']}" ({lead['description']}).

CONVERSACIÓN COMPLETA:
{conversation_text}

---

{config.FEEDBACK_CRITERIA}

---

INSTRUCCIONES DE EVALUACIÓN — LEELAS ANTES DE ESCRIBIR:
- Sé ESTRICTO. No regales puntos. Un 7 significa buen trabajo real, no "estuvo más o menos".
- Marcá CADA error, no solo los más grandes. Si algo estuvo mal, decilo con cita exacta del mensaje.
- No suavices críticas con frases como "podrías mejorar un poco". Di qué está mal y por qué.
- Para cada error, mostrá EXACTAMENTE cómo debería haberlo dicho.
- El objetivo es aprendizaje activo: el vendedor debe saber qué cambiar y cómo, no solo que algo estuvo mal.
- Si la conversación fue corta o superficial, bajá la puntuación significativamente.
- Si el vendedor no aplicó ninguna técnica VH, señalalo claramente.

Analizá la conversación completa y generá un feedback en español argentino con esta estructura EXACTA:

## 🎯 Puntuación General
[Número del 1 al 10 — sé honesto. Incluí una oración de síntesis que explique por qué ese número]

## ✅ Lo que hiciste bien (con evidencia)
[2-4 puntos. Para cada uno: citá exactamente qué dijo el vendedor y explicá por qué funcionó técnicamente]

## ❌ Errores y qué cambiar
[Para CADA error encontrado en la conversación:
- Citá exactamente qué dijo
- Explicá qué está mal y el impacto en la venta
- Mostrá cómo debería haberlo dicho (mensaje alternativo listo para usar)]

## 💡 La técnica VH más urgente para practicar
[La habilidad más débil identificada. Explicá qué es, por qué importa, y 2-3 ejercicios concretos para trabajarla]

## 📊 Puntajes por área
diagnostico_desapego: [0-10]
descubrimiento_acuerdos: [0-10]
empatia_escucha: [0-10]
ingenieria_preguntas: [0-10]
gestion_creencias: [0-10]
storytelling: [0-10]
pitch_personalizado: [0-10]
mentalidad: [0-10]

## 🔁 Plan de acción para la próxima práctica
[3 cambios específicos y concretos para aplicar la próxima vez que haga este roleplay. Ordenalos por impacto]
"""

    try:
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(config.GEMINI_MODEL)
        response = model.generate_content(feedback_prompt)
        feedback_text = response.text.strip()
    except Exception as e:
        logger.error("Gemini feedback error: %s", e)
        return jsonify({"ok": False, "error": "Error al generar feedback"}), 500

    # Parse score and section scores
    import re
    score = None
    sm = re.search(r"## 🎯 Puntuación General\s*\n.*?(\d+(?:\.\d+)?)\s*/?\s*10", feedback_text)
    if sm:
        try:
            score = float(sm.group(1))
        except Exception:
            pass

    section_keys = ["diagnostico_desapego","descubrimiento_acuerdos","empatia_escucha",
                    "ingenieria_preguntas","gestion_creencias","storytelling",
                    "pitch_personalizado","mentalidad"]
    section_scores = {}
    for k in section_keys:
        m = re.search(rf"{k}:\s*(\d+(?:\.\d+)?)", feedback_text)
        if m:
            section_scores[k] = float(m.group(1))

    try:
        database.close_session(session_id, feedback_text, score, _json.dumps(section_scores))
    except Exception as e:
        logger.error("close_session error: %s", e)
        return jsonify({"ok": False, "error": f"Error al guardar sesión: {e}"}), 500

    try:
        gamification = database.award_xp_and_badges(vendor["id"], score, sess["lead_id"])
    except Exception as e:
        logger.error("award_xp_and_badges error: %s", e)
        gamification = None

    return jsonify({"ok": True, "feedback": feedback_text, "score": score,
                    "section_scores": section_scores, "gamification": gamification})


@app.route("/chat/session/<int:session_id>")
def chat_view_session(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("vendor_login", next=request.path))
    sess = database.get_session(session_id)
    if not sess or sess["vendor_id"] != vendor["id"]:
        flash("Sesión no encontrada.", "danger")
        return redirect(url_for("chat"))
    lead = database.get_lead_by_id(sess["lead_id"])
    import json as _json
    messages = _json.loads(sess["messages_json"] or "[]")
    section_scores = {}
    if sess.get("section_scores"):
        try:
            section_scores = _json.loads(sess["section_scores"])
        except Exception:
            pass
    return render_template("chat_session.html", vendor=vendor, sess=sess,
                           lead=lead, messages=messages, section_scores=section_scores)


@app.route("/chat/session/<int:session_id>/delete", methods=["POST"])
def chat_delete_session(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False}), 401
    ok = database.delete_roleplay_session(session_id, vendor["id"])
    return jsonify({"ok": ok})


@app.route("/productor/papelera")
def productor_papelera():
    redir = _require_producer()
    if redir:
        return redir
    sessions = database.get_papelera_sessions()
    return render_template("papelera.html", sessions=sessions)


@app.route("/productor/papelera/restore/<int:session_id>", methods=["POST"])
def productor_papelera_restore(session_id: int):
    redir = _require_producer()
    if redir:
        return jsonify({"ok": False}), 403
    ok = database.restore_roleplay_session(session_id)
    return jsonify({"ok": ok})


# ── Roleplay Lanzamiento (Coach) ───────────────────────────────────────────────

LANZAMIENTO_PHASES = {
    "relacion": {
        "label": "Relación",
        "emoji": "❤️",
        "days": "Días 1–5",
        "goal": "Crear vínculo genuino usando E.P.P. (Escucho–Participo–Profundizo). Activar reciprocidad emocional, identificación y espejo.",
        "lead_behavior": "El lead está frío o neutral al principio. Responde con mensajes cortos. Se va abriendo sólo si el vendedor muestra calidez genuina, se abre primero y hace preguntas que tocan algo real de su vida.",
    },
    "descubrimiento": {
        "label": "Descubrimiento",
        "emoji": "🔍",
        "days": "Días 5–10",
        "goal": "Mapear los 7 puntos clave: objetivos, dolores, miedos, deseos, situación actual, problemas y costo de oportunidad.",
        "lead_behavior": "El lead ya tiene algo de confianza. Habla de su situación si le preguntan bien. Pero no profundiza solo — el vendedor tiene que hacer preguntas de re-pregunta. Si el vendedor no escucha o cambia de tema bruscamente, el lead se cierra.",
    },
    "siembra": {
        "label": "Siembra & Storytelling",
        "emoji": "🌱",
        "days": "Días 10–16",
        "goal": "Generar curiosidad, identificación e imaginación con mini historias reales. Romper creencias limitantes sutilmente. Invitar a la Clase 2 conectándola con el dolor del lead.",
        "lead_behavior": "El lead tiene curiosidad pero también escepticismo. Si la historia no lo conecta con su situación particular, no reacciona. Si la siembra es forzada o muy vendedora, se distancia. Reacciona bien cuando siente que el vendedor lo entendió DE VERDAD.",
    },
    "objeciones": {
        "label": "Objeciones & Recomendación",
        "emoji": "🛡️",
        "days": "Días 16–21",
        "goal": "Manejar objeciones con preguntas y acuerdos. Hacer la recomendación personalizada conectando el programa con los dolores específicos del lead.",
        "lead_behavior": "El lead tiene una o dos objeciones fuertes (precio, tiempo, duda de si funciona para él). Si el vendedor argumenta o presiona, el lead se cierra más. Si el vendedor pregunta y valida, el lead empieza a desarmarse solo.",
    },
}


LANZAMIENTO_PRESET_LEADS = {
    "relacion": [
        {
            "difficulty": "fácil", "emoji": "😊", "name": "Camila",
            "short": "Entró al taller, responde rápido y es amigable",
            "context": "Camila, 32 años, mamá de 2 hijos, trabaja media jornada como asistente administrativa. Entró al lanzamiento por una publicidad en Instagram. Responde rápido, le gusta la energía del taller. Aún no sabe bien qué hace VH. Es amigable y abierta.",
            "behavior_pattern": "ABIERTA — responde rápido, muestra interés",
            "alt_strategy": "Camila ya está abierta. Si el chat fluye bien, no cambies de canal. Si baja la energía o deja de responder 2+ días, mandá un audio de WhatsApp corto y cálido — el tono de voz conecta más que el texto con este perfil.",
        },
        {
            "difficulty": "medio", "emoji": "🤔", "name": "Sebastián",
            "short": "Educado pero reservado, hay que ganarse su confianza",
            "context": "Sebastián, 38 años, vendedor de autos. Entró al taller gratis por curiosidad. Es educado pero reservado — responde poco. No quiere que le vendan nada. Hay que ganarse su confianza antes de avanzar.",
            "behavior_pattern": "RESERVADO — respuestas cortas, no abre el juego solo",
            "alt_strategy": "Si después de 2-3 mensajes sigue respondiendo con frases solas ('sí', 'entiendo', 'ok'), no insistas por texto. Proponé una videollamada de 10 minutos sin presión: *'Sebastián, me parece que por acá no te termino de explicar bien. ¿Tenés 10 minutos esta semana para una llamada rápida, sin compromiso?'* — las personas reservadas muchas veces conectan mejor hablando que escribiendo.",
        },
        {
            "difficulty": "medio", "emoji": "😅", "name": "Rodrigo",
            "short": "Curioso pero se escapa cuando siente que le quieren vender",
            "context": "Rodrigo, 35 años, responsable de RRHH en una mediana empresa. Entró al taller porque le interesa el tema de equipos y crecimiento. Es receptivo cuando la conversación gira en torno a liderazgo y personas, pero en cuanto siente que le están vendiendo algo, pone distancia — responde más tarde, con menos palabras. Hay que construir relación desde su mundo (equipos, gestión, personas) antes de hablar de ventas.",
            "behavior_pattern": "CAUTELOSO CON LA VENTA — se abre en temas de su interés, se cierra si detecta intención comercial",
            "alt_strategy": "Si Rodrigo empieza a responder más tarde y con menos palabras, es señal de que detectó intención de venta. Volvé al tema que lo mueve: su trabajo, su equipo, su crecimiento. Una vez que vuelva a abrirse, considerá proponer una llamada corta encuadrada como 'quería entender mejor tu situación antes de contarte algo que podría aplicar a tu caso específico'.",
        },
        {
            "difficulty": "difícil", "emoji": "😶", "name": "Valeria",
            "short": "Casi no interactúa, responde con monosílabos",
            "context": "Valeria, 44 años, empleada pública. Se inscribió al taller pero casi no interactuó. Responde con monosílabos ('sí', 'ok', 'puede ser'). Hay que generar conexión desde cero con mucha paciencia.",
            "behavior_pattern": "MONOSILÁBICA — respuestas de 1 palabra, casi no interactúa",
            "alt_strategy": "Con Valeria, el chat por texto solo no va a funcionar. Después de 2-3 monosílabos, cambiá de canal: enviá un audio personal de WhatsApp (30 segundos, cálido, sin presión) preguntando algo puntual sobre su situación. Si tampoco responde al audio en 48hs, proponé una videollamada corta: *'Valeria, sería más fácil contarte todo en 10 minutos por videollamada, ¿te parece? Elegís vos el día y hora.'* — el cambio de canal rompe el patrón de no-respuesta.",
        },
        {
            "difficulty": "difícil", "emoji": "🙄", "name": "Lorena",
            "short": "Ya compró cursos que no le sirvieron, muy escéptica",
            "context": "Lorena, 43 años, vendedora de seguros hace 12 años. Entró al taller 'a ver qué onda'. Ya invirtió en 2 cursos online de ventas que 'no le aportaron nada'. Cada vez que se menciona algo que suene a formación o programa, responde con frases como 'eso ya lo sé', 'todos dicen lo mismo' o 'los cursos no funcionan para mi rubro'. El cinismo es su mecanismo de defensa.",
            "behavior_pattern": "ESCÉPTICA CON HISTORIAL — responde con cinismo ante cualquier promesa o contenido de ventas",
            "alt_strategy": "Con Lorena no funciona hablar del programa directamente. Primero hay que validar su experiencia: 'Entiendo, la mayoría de los cursos son genéricos y no sirven de nada'. Luego trabajar el descubrimiento profundo de su situación actual. Si después de 4-5 intercambios sigue muy cerrada, proponé hablar con un alumno que tenga su mismo perfil (vendedor con experiencia) — el testimonio de par a par rompe el escepticismo mejor que cualquier argumento tuyo.",
        },
        {
            "difficulty": "difícil", "emoji": "🤐", "name": "Esteban",
            "short": "Responde largo pero siempre evade comprometerse",
            "context": "Esteban, 55 años, gerente de área que quiere lanzarse como consultor independiente pero tiene miedo. Escribe mensajes largos y bien redactados — parece abierto — pero cuando llegás a algo concreto ('¿qué te frena?', '¿cuándo sería buen momento?') siempre desvía: habla de factores externos, de que 'el mercado está difícil', de que 'primero tiene que terminar de ordenar unas cosas'. Nunca dice no, pero nunca avanza.",
            "behavior_pattern": "EVASIVO VERBAL — escribe mucho, comparte poco de verdad, evita todo compromiso concreto",
            "alt_strategy": "Con Esteban, el texto juega en su contra — le permite construir respuestas largas y evasivas sin presión. Si después de varias respuestas notás que habla mucho pero no dice nada concreto, proponé una videollamada: 'Esteban, me parece que hay algo importante que no te estoy pudiendo transmitir bien por acá. ¿Tenés 20 minutos para que lo hablemos? Quiero entender de verdad qué te está frenando.' — en videollamada es mucho más difícil evadir.",
        },
    ],
    "descubrimiento": [
        {
            "difficulty": "fácil", "emoji": "💬", "name": "Paula",
            "short": "Habla mucho y comparte sus dolores fácilmente",
            "context": "Paula, 29 años, emprendedora que vende productos de limpieza naturales. Quiere crecer pero no sabe cómo. Habla mucho de sus problemas: ingresos estancados, cansancio, siente que trabaja sola. Ideal para practicar descubrimiento activo.",
            "behavior_pattern": "ABIERTA Y HABLADORA — comparte sus dolores sin que le pregunten",
            "alt_strategy": "Paula habla mucho — el riesgo no es que no responda sino que la conversación se vaya por las ramas. Si después de varios mensajes el vendedor no tiene claro el dolor principal, proponé una llamada corta para 'tener más claridad y poder orientarla mejor': concentra la energía y permite hacer descubrimiento profundo de forma más eficiente.",
        },
        {
            "difficulty": "medio", "emoji": "🔒", "name": "Marcos",
            "short": "No abre el juego solo, hay que preguntar bien",
            "context": "Marcos, 41 años, contador con consultora propia pero ingresos irregulares. Es reservado con sus miedos. Responde a preguntas concretas pero no abre el juego solo. Hay que preguntar bien para que profundice.",
            "behavior_pattern": "RESERVADO CONCRETO — responde si le preguntás bien, pero no abre solo",
            "alt_strategy": "Si Marcos responde a preguntas pero sus respuestas son siempre cortas y racionales (nunca emocionales), el texto por chat limita el descubrimiento. Proponé una videollamada de 15 min presentándola como 'para entender mejor tu situación puntual y ver si el programa tiene sentido para vos específicamente' — Marcos como contador valora que le dediquen tiempo en serio.",
        },
        {
            "difficulty": "medio", "emoji": "🌀", "name": "Claudia",
            "short": "Habla de sus productos pero evita hablar de sus problemas reales",
            "context": "Claudia, 38 años, emprendedora de cosmética natural con marca propia. Está orgullosa de sus productos y los describe en detalle, pero cuando las preguntas van hacia sus dolores reales (ingresos, escala, distribución) desvía la conversación hacia sus logros. No es que mienta — es que hablarle del dolor se siente amenazante para su identidad de emprendedora. Hay que redirigirla al dolor sin que se sienta cuestionada.",
            "behavior_pattern": "DESVÍA AL LOGRO — cuando preguntás por sus problemas, habla de sus productos y éxitos",
            "alt_strategy": "Con Claudia el riesgo es hacer descubrimiento superficial — ella responde mucho pero sin ir al fondo. Si después de 3-4 mensajes solo hablaste de sus productos y no de sus problemas, cambiá el ángulo: 'Claudia, con todo lo que lograste, ¿qué es lo que todavía no te cierra como te gustaría?' — reformulá el dolor como algo que 'le falta completar', no como un fracaso. En videollamada podés profundizar mucho más sin que ella prepare mentalmente sus respuestas.",
        },
        {
            "difficulty": "difícil", "emoji": "🛡️", "name": "Romina",
            "short": "Se pone a la defensiva si preguntás mucho",
            "context": "Romina, 36 años, vendedora freelance de seguros. Interpreta las preguntas como un interrogatorio. Si preguntás mucho dice '¿para qué necesitás saber eso?' o 'estoy bien como estoy'. Hay que avanzar muy despacio y con mucha empatía.",
            "behavior_pattern": "DEFENSIVA — se cierra si siente que la están interrogando",
            "alt_strategy": "Con Romina, el chat puede volverse tenso fácilmente. Si nota resistencia (respuestas cortas + tono defensivo), no insistas por texto — eso escala la tensión. En cambio, bajá la guardia vos primero: contale algo personal o una historia propia antes de pedir que ella cuente. O propone una videollamada diciéndole que 'así es más fácil charlar sin que parezca un formulario' — el cara a cara reduce la sensación de interrogatorio.",
        },
        {
            "difficulty": "difícil", "emoji": "👑", "name": "Antonella",
            "short": "Tiene ego — si no la tratás como experta, se cierra",
            "context": "Antonella, 31 años, influencer con 40K seguidores en Instagram y ingresos irregulares que no logra estabilizar. Sabe mucho de contenido y redes, pero tiene un punto ciego con la parte comercial. Si el vendedor le hace preguntas que implican que ella no sabe algo, reacciona con distancia o sarcasmo ('eso ya lo sé', 'obvio que lo intenté'). Hay que hacer descubrimiento tratándola como alguien con expertise, no como alguien que necesita ayuda.",
            "behavior_pattern": "EGO ALTO — se cierra inmediatamente si siente que la subestiman",
            "alt_strategy": "Con Antonella la clave es el encuadre: hacé las preguntas desde un lugar de curiosidad genuina por su mundo, no de diagnóstico. 'Alguien con tu audiencia, ¿cómo está manejando la parte de monetización?' suena diferente a '¿cuánto facturás?'. Si por texto la energía se vuelve tensa, en videollamada podés trabajar desde el respeto mutuo con mucho más matices.",
        },
        {
            "difficulty": "difícil", "emoji": "🏰", "name": "Jorge",
            "short": "Cree que su problema es único, no conecta con lo genérico",
            "context": "Jorge, 47 años, dueño de una PyME de impresión gráfica. Tiene problemas de ingresos irregulares y dependencia de 2 o 3 clientes grandes. Responde bien a preguntas superficiales pero cuando el vendedor intenta ir al fondo ('¿y eso cómo te afecta a vos?', '¿qué te genera eso?') se cierra: 'mi caso es muy específico', 'el rubro gráfico es diferente', 'eso aplica para otros negocios'. No es que no quiera hablar — es que desconfía de soluciones que no sean 100% de su rubro.",
            "behavior_pattern": "EL CASO ÚNICO — bloquea el descubrimiento profundo con 'mi rubro es diferente'",
            "alt_strategy": "Con Jorge tenés que validar la especificidad antes de preguntar: 'Entiendo que la gráfica tiene sus particularidades. ¿Me contás cómo funciona en tu caso específicamente?' — pedirle que te eduque sobre su rubro lo abre. Después de que explique, podés redirigir: 'Entiendo. Y en ese contexto, ¿qué es lo que más te preocupa hoy?'. En videollamada podés profundizar con mucho más ritmo y redirigir sin que el texto quede frío.",
        },
    ],
    "siembra": [
        {
            "difficulty": "fácil", "emoji": "🌱", "name": "Florencia",
            "short": "Receptiva, escucha bien y se emociona con las historias",
            "context": "Florencia, 27 años, quiere salir de su trabajo en relación de dependencia. Ya confía en el vendedor. Escucha, hace preguntas, se emociona con historias. Es el momento de sembrar bien: conectar su situación con casos de éxito y el programa.",
            "behavior_pattern": "RECEPTIVA Y EMOCIONAL — se conecta con historias, hace preguntas",
            "alt_strategy": "Florencia está en el canal ideal. Si querés potenciar la siembra, compartile un audio o video testimonial corto de un alumno con perfil similar (empleada que quería independencia). El formato visual/auditivo amplifica el impacto emocional mucho más que el texto.",
        },
        {
            "difficulty": "medio", "emoji": "😐", "name": "Gustavo",
            "short": "Le entran algunos hooks pero dice 'mi caso es diferente'",
            "context": "Gustavo, 45 años, dueño de una ferretería. Quiere crecer pero no conecta con historias ajenas fácilmente. Dice 'mi caso es diferente'. Hay que sembrar con ejemplos muy específicos parecidos a su situación.",
            "behavior_pattern": "RACIONAL DISTANTE — se desconecta de historias que no son idénticas a la suya",
            "alt_strategy": "Si por chat Gustavo sigue diciendo 'mi caso es diferente' después de 2 historias, el formato texto no está funcionando para él. Propone una videollamada breve y encuadrala como 'quiero entender bien qué hace tu negocio para ver si tengo casos similares que te pueda mostrar' — esto le da lo que necesita (personalización) y te permite sembrar mejor con su propio lenguaje.",
        },
        {
            "difficulty": "medio", "emoji": "📊", "name": "Ramiro",
            "short": "Solo conecta con historias de equipos de ventas, no de emprendedores",
            "context": "Ramiro, 40 años, director comercial de una empresa mediana. Tiene a cargo un equipo de 8 vendedores. Le interesa el programa para él y para su equipo, pero cuando las historias son de emprendedores solos ('un chico que armó su negocio desde cero'), se desconecta: 'yo trabajo en empresa, no es lo mismo'. Solo engancha cuando los casos son de managers, líderes de equipos o empresas.",
            "behavior_pattern": "SELECTIVO CON LOS EJEMPLOS — solo conecta con casos de su mundo corporativo",
            "alt_strategy": "Con Ramiro tenés que adaptar los casos: antes de contar una historia, alineá el perfil ('Ramiro, tengo el caso de un director comercial que tenía un equipo de 6 personas y el mismo problema que describís...'). Si por texto no tenés casos exactamente de su perfil, en videollamada podés adaptarlos con más libertad y leer cómo reacciona en tiempo real.",
        },
        {
            "difficulty": "difícil", "emoji": "🧱", "name": "Alejandro",
            "short": "No cree en los cursos, mucha resistencia",
            "context": "Alejandro, 50 años, gerente de ventas con 20 años de experiencia. Cuando mencionás el programa dice 'eso es para gente que no sabe'. No cree en la formación. Hay que sembrar con prueba social muy fuerte sin mencionar el programa directamente.",
            "behavior_pattern": "MUY RESISTENTE — descarta la formación, cree que ya sabe todo",
            "alt_strategy": "Con Alejandro, sembrar por texto es muy difícil porque puede ignorar o refutar con 1 línea. El cambio de canal más efectivo acá es proponerle hablar con un alumno que tenga su mismo perfil (gerente o dueño de empresa con experiencia) — 'Alejandro, tengo un alumno que pensaba exactamente lo mismo que vos antes de entrar, ¿te interesaría charlar 5 minutos con él?' Esto saca la siembra de tu boca y la pone en alguien con quien se pueda identificar.",
        },
        {
            "difficulty": "difícil", "emoji": "😔", "name": "Lucas",
            "short": "Fracasó antes, corta las historias de éxito con 'eso no aplica para mí'",
            "context": "Lucas, 28 años, emprendedor digital que intentó 2 negocios que no funcionaron. Tiene mucho miedo de ilusionarse y volver a fracasar. Cuando contás historias de éxito, reacciona con frases como 'eso es porque tenían más recursos', 'yo ya lo intenté y no funcionó', 'eso no aplica para mí'. No rechaza el contenido por soberbia — lo rechaza por autoprotección.",
            "behavior_pattern": "MIEDO A ILUSIONARSE — corta cada historia de éxito con una razón por la que 'su caso es diferente'",
            "alt_strategy": "Con Lucas la siembra directa no funciona — cada historia de éxito activa su mecanismo de defensa. En cambio, usá el 'antipatrón': antes de contar el caso de éxito, validá el fracaso ('Lo que describís es lo más normal del mundo, la mayoría que arranca sin un método correcto pasa por eso'). Después del caso, preguntá qué parte siente que aplica, en lugar de afirmar que aplica. En videollamada podés manejar el tono emocional con mucho más precisión.",
        },
        {
            "difficulty": "difícil", "emoji": "🔬", "name": "Patricia",
            "short": "20 años de experiencia, solo conecta con contenido muy avanzado",
            "context": "Patricia, 52 años, consultora independiente con 20 años de experiencia en empresas. Leyó varios libros de ventas, hizo cursos. Cuando contás historias o siembras algo, responde con 'eso ya lo sé' o 'eso es básico'. Solo reacciona bien si el contenido es sorprendente o muy específico para ella. Tiene una barrera de 'ya sé todo esto' que hay que romper sin atacar su identidad.",
            "behavior_pattern": "BARRERA DEL EXPERTO — descarta la siembra si la percibe como contenido básico",
            "alt_strategy": "Con Patricia la siembra tiene que ser disruptiva: en lugar de contar casos de éxito, planteá una pregunta que desafíe algo que ella cree que ya sabe ('Patricia, ¿sabés cuál es la diferencia entre un vendedor que factura 300K y uno que factura 3M, si el producto es el mismo?'). Esto la pone en modo curioso en lugar de modo 'ya sé'. En videollamada podés generar ese momento de insight con más control y profundidad.",
        },
    ],
    "objeciones": [
        {
            "difficulty": "fácil", "emoji": "💰", "name": "Natalia",
            "short": "Una sola objeción: el precio",
            "context": "Natalia, 33 años, diseñadora gráfica independiente. Quiere entrar al programa, ya casi convencida. Su única objeción es el precio: 'es mucho'. No tiene objeciones de tiempo ni credibilidad. Practicá el manejo del precio con elegancia y sin dar descuentos.",
            "behavior_pattern": "CASI CONVENCIDA — una sola objeción de precio, receptiva",
            "alt_strategy": "Si Natalia responde bien por texto, manejá la objeción del precio ahí. Pero si después de 2 intentos de manejar el precio sigue sin decidir, proponé una llamada de 10 minutos: *'Natalia, creo que por chat me falta transmitirte bien el valor. ¿Tenés 10 minutos para que te cuente en vivo cómo le fue a alguien con el mismo perfil tuyo?'* — muchas veces el precio deja de ser objeción cuando la persona escucha un caso real hablado.",
        },
        {
            "difficulty": "medio", "emoji": "🤯", "name": "Fernando",
            "short": "Lo tiene que pensar + consultar con su mujer",
            "context": "Fernando, 37 años, comerciante con local propio. Le interesa el programa pero tiene dos objeciones: 'lo tengo que pensar' y 'lo tengo que hablar con mi mujer'. No es que no quiera — necesita validación externa y más tiempo. Practicá el desapego y cierres suaves.",
            "behavior_pattern": "NECESITA VALIDACIÓN EXTERNA — posterga con 'lo hablo con mi mujer'",
            "alt_strategy": "Con Fernando, la clave es la pareja. Por chat es difícil llegar a ella. Ofrecé incluirla directamente: *'Fernando, con gusto te explico lo mismo a los dos juntos en una videollamada de 15 minutos. Así tu mujer tiene toda la info y lo pueden decidir con claridad juntos.'* — esto saca la objeción de la pareja del juego porque la traés al proceso en vez de luchar contra ella.",
        },
        {
            "difficulty": "medio", "emoji": "💔", "name": "Carla",
            "short": "Su pareja no la apoya, la objeción es emocional",
            "context": "Carla, 34 años, empleada administrativa que quiere independizarse y emprender. Está muy convencida del programa, pero su pareja cree que 'gastar en cursos es tirar plata'. La objeción no es racional — es miedo a conflicto con su pareja. Dice 'yo quiero pero mi familia no entiende', 'si entro va a haber quilombo en casa'.",
            "behavior_pattern": "OBJECIÓN EMOCIONAL FAMILIAR — el freno no es el precio ni el tiempo, es el apoyo del entorno",
            "alt_strategy": "Con Carla no sirve responder la objeción del precio porque ese no es el problema real. Primero validá la situación ('Entiendo, es difícil cuando las personas que querés no ven lo mismo que vos'). Luego trabajá su certeza interna: '¿Qué necesitarías vos para estar segura de que esto es lo correcto, independientemente de lo que piense tu pareja?'. Si la conversación se pone cargada emocionalmente, una videollamada te permite manejar el tono con mucho más cuidado.",
        },
        {
            "difficulty": "difícil", "emoji": "🔥", "name": "Nicolás",
            "short": "Múltiples objeciones en cadena, la más difícil",
            "context": "Nicolás, 42 años, ex-emprendedor que tuvo un negocio y le fue mal. Múltiples objeciones en cadena: 'es caro', 'no tengo tiempo', 'ya probé cosas así y no funcionaron', 'no sé si esto es para mí'. Cuando resolvés una, aparece la siguiente. Practicá persistencia con desapego.",
            "behavior_pattern": "OBJECIONES EN CADENA — cada vez que resolvés una, aparece otra",
            "alt_strategy": "Cuando las objeciones se encadenan en texto, es una señal de que el canal no está funcionando — por chat es fácil objetar porque no hay costo social. Cambiá de estrategia: en vez de seguir respondiendo objeciones, proponé parar: *'Nicolás, veo que tenés varias dudas importantes — por chat es difícil que te las pueda responder bien. ¿Hacemos una llamada de 15 minutos y las vemos todas juntas?'* — la llamada te da control del ritmo, no puede objetar todo al mismo tiempo, y el tono de voz baja la guardia.",
        },
        {
            "difficulty": "difícil", "emoji": "⏱️", "name": "Pablo",
            "short": "No tiene tiempo — y es verdad, trabaja 60hs por semana",
            "context": "Pablo, 46 años, médico con consultorio propio y guardia en hospital. La objeción del tiempo no es excusa — es real. Trabaja 60 horas por semana y tiene familia. Cuando le decís 'son solo 2 horas por semana', responde 'en serio, no tengo NI 2 horas libres'. Es inteligente y detecta rápido cuando alguien minimiza su situación.",
            "behavior_pattern": "TIEMPO GENUINAMENTE LIMITADO — la objeción no es excusa, es real y se cierra si la minimizás",
            "alt_strategy": "Con Pablo no podés decir 'son solo 2 horas' porque él sabe que su tiempo es más escaso que el de la mayoría. En cambio, validá completamente: 'Entiendo que tu situación es diferente a la mayoría'. Luego llevá la conversación al costo de oportunidad: '¿Cuánto te está costando hoy no tener esto resuelto?' — si el dolor es suficientemente grande, el tiempo se encuentra. En videollamada podés hacer este trabajo con mucho más precisión emocional.",
        },
        {
            "difficulty": "difícil", "emoji": "🌀", "name": "Silvia",
            "short": "Siempre pide más tiempo para decidir, no importa qué le digas",
            "context": "Silvia, 39 años, contadora con buen pasar económico. Tiene el dinero, el interés y el tiempo. El problema es la indecisión crónica: siempre hay una razón para esperar. 'Déjame pensar hasta el jueves', 'la semana que viene te digo', 'espero ver cómo está el dólar'. Cuando el jueves llega, pide otro plazo. No está mintiendo — genuinamente se paraliza ante decisiones de inversión.",
            "behavior_pattern": "INDECISIÓN CRÓNICA — pide más tiempo indefinidamente, no importa qué objeciones resuelvas",
            "alt_strategy": "Con Silvia el peligro es entrar en el juego del 'cuando esté lista'. No existe ese momento — hay que crear una condición de urgencia genuina. En texto podés usar escasez real ('El precio sube el viernes', 'quedan X lugares'). Si sigue posponiendo, preguntá directamente: 'Silvia, en serio te pregunto: ¿qué tendría que pasar para que digas que sí hoy?' — esa pregunta externaliza la decisión y la obliga a articular lo que realmente le falta.",
        },
    ],
}


@app.route("/chat/lanzamiento")
def chat_lanzamiento():
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("vendor_login", next=request.path))
    sessions = database.lanzamiento_coach_get_vendor_sessions(vendor["id"])
    return render_template("chat_lanzamiento.html", vendor=vendor,
                           sessions=sessions, phases=LANZAMIENTO_PHASES,
                           preset_leads=LANZAMIENTO_PRESET_LEADS)


@app.route("/lanzamiento/roleplay")
def roleplay_lanzamiento():
    redir = _require_vendor()
    if redir: return redir
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("vendor_login", next=request.path))
    sessions = database.lanzamiento_coach_get_vendor_sessions(vendor["id"])
    return render_template("roleplay_lanzamiento.html", vendor=vendor,
                           sessions=sessions, phases=LANZAMIENTO_PHASES,
                           preset_leads=LANZAMIENTO_PRESET_LEADS)


@app.route("/videollamadas")
def videollamadas():
    redir = _require_vendor()
    if redir: return redir
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("vendor_login", next=request.path))
    return render_template("videollamadas.html", vendor=vendor)


@app.route("/llamadas")
def llamadas():
    redir = _require_vendor()
    if redir: return redir
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("vendor_login", next=request.path))
    return render_template("llamadas.html", vendor=vendor)


@app.route("/chat/lanzamiento/session/start", methods=["POST"])
def chat_lanzamiento_start():
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401
    data = request.get_json()
    mode = data.get("mode", "roleplay")  # 'roleplay' or 'asistente'
    phase = data.get("phase", "relacion")
    lead_name = (data.get("lead_name") or "").strip()
    lead_context = (data.get("lead_context") or "").strip()

    if phase not in LANZAMIENTO_PHASES:
        return jsonify({"ok": False, "error": "Fase inválida"}), 400

    session_id = database.lanzamiento_coach_create(
        vendor_id=vendor["id"], mode=mode, phase=phase,
        lead_name=lead_name, lead_context=lead_context,
    )
    return jsonify({"ok": True, "session_id": session_id})


@app.route("/chat/lanzamiento/session/<int:session_id>/message", methods=["POST"])
def chat_lanzamiento_message(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    sess = database.lanzamiento_coach_get(session_id)
    if not sess or sess["vendor_id"] != vendor["id"]:
        return jsonify({"ok": False, "error": "Sesión no válida"}), 403
    if sess["status"] != "active":
        return jsonify({"ok": False, "error": "Sesión cerrada"}), 400

    import google.generativeai as genai
    import json as _json

    # Support both JSON (text only) and multipart (text + optional file)
    temp_file_path = None
    gemini_file_obj = None
    media_file_desc = ""

    content_type = request.content_type or ""
    if "multipart" in content_type:
        user_text = (request.form.get("text") or "").strip()
        uploaded = request.files.get("file")
        if uploaded and uploaded.filename:
            ext = uploaded.filename.rsplit(".", 1)[-1].lower() if "." in uploaded.filename else "jpg"
            import tempfile
            suffix = f".{ext}"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                uploaded.save(tmp)
                temp_file_path = tmp.name
            media_file_desc = f"{uploaded.filename}"
            # Upload to Gemini Files API
            try:
                from google import genai as _gc
                from google.genai import types as _gct
                mime_map = {
                    "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                    "webp": "image/webp", "gif": "image/gif",
                    "mp4": "video/mp4", "mov": "video/quicktime",
                    "avi": "video/x-msvideo", "mkv": "video/x-matroska",
                    "webm": "video/webm", "m4v": "video/mp4",
                }
                mime = mime_map.get(ext, "image/jpeg")
                _client = _gc.Client(api_key=config.GEMINI_API_KEY)
                gemini_file_obj = _client.files.upload(
                    file=temp_file_path,
                    config=_gct.UploadFileConfig(mime_type=mime),
                )
                # Wait for video processing
                import time as _time
                waited = 0
                while gemini_file_obj.state.name == "PROCESSING" and waited < 120:
                    _time.sleep(5); waited += 5
                    gemini_file_obj = _client.files.get(name=gemini_file_obj.name)
                if gemini_file_obj.state.name == "FAILED":
                    gemini_file_obj = None
                    media_file_desc = f"{uploaded.filename} (no se pudo procesar)"
            except Exception as _fe:
                logger.warning("Could not upload file to Gemini: %s", _fe)
                gemini_file_obj = None
    else:
        data = request.get_json() or {}
        user_text = (data.get("text") or "").strip()

    if not user_text and not gemini_file_obj:
        return jsonify({"ok": False, "error": "Mensaje o archivo requerido"}), 400

    phase_info = LANZAMIENTO_PHASES.get(sess["phase"], LANZAMIENTO_PHASES["relacion"])
    messages = _json.loads(sess["messages_json"] or "[]")

    if sess["mode"] == "roleplay":
        # ── MODO ROLEPLAY: AI actúa como lead ──────────────────────────────
        lead_name = sess["lead_name"] or "Lead"
        messages.append({"role": "vendor", "text": user_text})

        history_text = "\n".join(
            f"{'VENDEDOR' if m['role'] == 'vendor' else lead_name.upper()}: {m['text']}"
            for m in messages
        )

        # Detect lead behavior pattern from conversation history for roleplay coaching
        lead_msg_count = sum(1 for m in messages if m["role"] == "lead")
        vendor_msg_count = sum(1 for m in messages if m["role"] == "vendor")
        lead_msgs = [m["text"] for m in messages if m["role"] == "lead"]
        short_responses = sum(1 for t in lead_msgs if len(t.split()) <= 3)
        behavior_note = ""
        if lead_msg_count >= 2 and short_responses >= lead_msg_count * 0.6:
            behavior_note = "\n⚠️ COMPORTAMIENTO ACTUAL: El lead está respondiendo con mensajes muy cortos. Mantené ese patrón monosilábico — hace que el vendedor deba esforzarse más para generar conexión."

        system_prompt = f"""Estás haciendo un roleplay de práctica de ventas para {vendor['name']}, vendedor del lanzamiento digital de Valentín Hernández (VH).

Interpretás el personaje: {lead_name}
Perfil del lead: {sess['lead_context'] or 'Lead genérico que llegó al lanzamiento de VH. Tiene interés pero también dudas.'}

FASE DE PRÁCTICA: {phase_info['label']} ({phase_info['days']})
Objetivo del vendedor en esta fase: {phase_info['goal']}
Cómo debés comportarte en esta fase: {phase_info['lead_behavior']}
{behavior_note}

REGLAS ESTRICTAS:
- Respondé SOLO como {lead_name}. Nunca rompas el personaje.
- Mensajes cortos: 1 a 3 oraciones máximo. Tono de WhatsApp, argentino informal.
- No digas que sos una IA.
- Si el vendedor usa E.P.P. bien (escucha, participa, profundiza) → abrís un poco más.
- Si el vendedor presiona, vende antes de tiempo, o ignora lo que dijiste → te cerrás, respondés más seco.
- Si el vendedor hace una siembra genuina y conectada con algo que dijiste → mostrás interés real.
- Si el vendedor intenta recomendar el programa antes de haber construido relación y descubrimiento → ponés resistencia ("no sé, no conozco mucho", "voy a pensar").
- Si el vendedor propone un cambio de canal (llamada, videollamada, audio) que tiene sentido para el momento → reaccioná de forma coherente con tu personalidad (Valeria dudaría, Camila aceptaría fácil, Sebastián pediría que sea breve).

Historial:
{history_text}

Respondé con EXACTAMENTE este formato (sin agregar nada más):

LEAD:
[tu respuesta como {lead_name}, 1-3 oraciones, tono WhatsApp argentino informal]

COACHING:
✅ **Bien:** [una cosa concreta que hizo bien el vendedor en este último mensaje — citá las palabras exactas si es posible]
⚠️ **A mejorar:** [una cosa específica que podría haber hecho mejor — sé concreto, no genérico]
💡 **Alternativa:** [una versión mejorada o una pregunta diferente que hubiera funcionado mejor, lista para copiar]
🎯 **Por qué:** [en 1 oración: el impacto que tiene ese cambio en el lead]"""

        try:
            genai.configure(api_key=config.GEMINI_API_KEY)
            model = genai.GenerativeModel(config.GEMINI_MODEL)
            response = model.generate_content(system_prompt)
            raw = response.text.strip()
        except Exception as e:
            logger.error("Gemini lanzamiento roleplay error: %s", e)
            return jsonify({"ok": False, "error": "Error al generar respuesta"}), 500
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass

        # Parse lead reply and coaching tip from combined response
        lead_reply = raw
        coaching_tip = None
        if "COACHING:" in raw:
            parts = raw.split("COACHING:", 1)
            lead_part = parts[0]
            coaching_tip = parts[1].strip()
            # Clean lead reply
            if "LEAD:" in lead_part:
                lead_reply = lead_part.split("LEAD:", 1)[1].strip()
            else:
                lead_reply = lead_part.strip()
        elif "LEAD:" in raw:
            lead_reply = raw.split("LEAD:", 1)[1].strip()

        messages.append({"role": "lead", "text": lead_reply})
        database.lanzamiento_coach_update_messages(session_id, _json.dumps(messages))
        return jsonify({"ok": True, "reply": lead_reply, "role": "lead",
                        "coaching_tip": coaching_tip})

    else:
        # ── MODO ASISTENTE EN VIVO ──────────────────────────────────────────
        lead_name = sess["lead_name"] or "el lead"

        # Build conversation history
        history_parts = []
        for m in messages:
            if m["role"] == "vendor_query":
                history_parts.append(f"[{vendor['name']} consultó]: {m['text']}")
            elif m["role"] == "coach_suggestion":
                history_parts.append(f"[Coach respondió]: {m['text'][:300]}...")
        history_text = "\n".join(history_parts[-12:]) if history_parts else "— Primera consulta —"

        has_file = bool(media_file_desc)
        file_identification = ""
        if has_file:
            file_identification = f"""

━━━ IDENTIFICACIÓN EN LA CAPTURA/VIDEO ━━━
El archivo adjunto es una captura de pantalla o grabación del WhatsApp de {vendor['name']}.

REGLA PRINCIPAL — identificá por el COLOR DE FONDO del mensaje (es la más confiable):
- Mensajes con fondo VERDE → son de {vendor['name']} (EL VENDEDOR). Siempre.
- Mensajes con fondo GRIS o BLANCO → son de {lead_name} (EL LEAD/PROSPECTO). Siempre.

Regla secundaria — posición (usala solo si el color no es claro):
- Lado DERECHO de la pantalla → {vendor['name']} (vendedor).
- Lado IZQUIERDO de la pantalla → {lead_name} (lead).

NUNCA inviertas los roles. VERDE = vendedor, GRIS = lead. Esto es fundamental."""

        # ── Behavior analysis for live assistant ────────────────────────────
        # Analyze lead message patterns from conversation history
        lead_messages_hist = [m["text"] for m in messages if m["role"] == "lead"]
        vendor_messages_hist = [m["text"] for m in messages if m["role"] == "vendor_query"]
        total_lead_msgs = len(lead_messages_hist)
        short_lead_msgs = sum(1 for t in lead_messages_hist if len(t.split()) <= 4)
        behavior_analysis = ""
        if total_lead_msgs >= 2:
            monosyllabic_pct = short_lead_msgs / total_lead_msgs
            if monosyllabic_pct >= 0.6:
                behavior_analysis = "🚨 PATRÓN DETECTADO: LEAD MONOSILÁBICO — El lead está respondiendo con mensajes muy cortos (1-4 palabras) en la mayoría de los intercambios. Esto indica baja apertura o que el canal de texto no está funcionando."
            elif monosyllabic_pct >= 0.4:
                behavior_analysis = "⚠️ PATRÓN DETECTADO: RESPUESTAS CORTAS — El lead alterna entre respuestas cortas y un poco más largas. Hay apertura parcial pero el canal de texto no está generando suficiente conexión."
            elif total_lead_msgs >= 3 and all(len(t.split()) >= 8 for t in lead_messages_hist[-2:]):
                behavior_analysis = "✅ PATRÓN DETECTADO: LEAD ACTIVO Y ABIERTO — El lead está respondiendo con mensajes largos y elaborados. Hay conexión real. Aprovechá el momentum."
        if not behavior_analysis and user_text:
            lower = user_text.lower()
            if any(w in lower for w in ["no responde", "sin respuesta", "visto", "ignoró", "dejó de", "ghosting", "no contesta"]):
                behavior_analysis = "🚨 PATRÓN DETECTADO: LEAD FANTASMA — El lead dejó de responder o está ignorando los mensajes. El canal de texto claramente no está funcionando."
            elif any(w in lower for w in ["monosílabo", "sí solo", "ok solo", "responde poco", "poco", "corto"]):
                behavior_analysis = "🚨 PATRÓN DETECTADO: LEAD MONOSILÁBICO — Responde con palabras sueltas. El texto por sí solo no va a generar la conexión necesaria."
            elif any(w in lower for w in ["objecion", "objeción", "objeciones", "no quiere", "duda", "pero", "aunque"]):
                behavior_analysis = "⚠️ PATRÓN DETECTADO: OBJECIONES ACTIVAS — El lead está poniendo resistencia. Evaluar si el canal texto es el adecuado o si conviene proponer un cambio."

        coach_prompt = f"""Sos el mejor asesor comercial y coach de ventas del equipo de Valentín Hernández (VH). Tenés dominio absoluto de la metodología: relación profunda, descubrimiento, siembra, storytelling, manejo de objeciones y cierre con desapego. Tu misión es ayudar a {vendor['name']} a cerrar más ventas durante el lanzamiento de 21 días.

━━━ CONTEXTO ━━━

Vendedor: {vendor['name']}
Lead: {lead_name}
Perfil del lead: {sess['lead_context'] or 'Sin contexto registrado aún.'}
Fase del lanzamiento: {phase_info['label']} ({phase_info['days']}) — {phase_info['goal']}
{f'ANÁLISIS DE COMPORTAMIENTO: {behavior_analysis}' if behavior_analysis else ''}{file_identification}

━━━ HISTORIAL DE ESTA SESIÓN ━━━
{history_text}

━━━ SITUACIÓN ACTUAL ━━━
{user_text + (f' [Archivo adjunto: {media_file_desc}]' if has_file else '')}

━━━ OBJETIVO DEL LANZAMIENTO ━━━
Semanas 1-2: sembrar, elevar conciencia, invitar a Clase 2 gratuita conectándola con el dolor del lead.
Semana 3: invitar Clase 3 final, luego recomendación personalizada del programa.
El lead debe sentir siempre que él está eligiendo. Nunca que lo están vendiendo.

━━━ RESPUESTA REQUERIDA ━━━

Respondé con EXACTAMENTE esta estructura:

---

📊 **LECTURA DE LA SITUACIÓN**
[2-3 oraciones: ¿Dónde está emocionalmente este lead? ¿Qué reveló (consciente o inconscientemente) en lo que el vendedor describió o en la imagen? ¿Qué está buscando realmente? Si hay un patrón de comportamiento detectado, nombralo claramente.]

🎯 **ESTRATEGIA PARA ESTE LEAD**
[La estrategia específica para las próximas 24-48 horas con este lead particular. No genérica — basada en su comportamiento real. Incluí qué etapa priorizar, por qué, y qué resultado buscar.]

---

**OPCIÓN A** — 🤝 Conexión y vínculo
> [Mensaje completo, cálido, argentino informal. Listo para copiar. Aplica E.P.P.: escucha + apertura personal + pregunta profunda. Sin emojis forzados.]

**OPCIÓN B** — 🔍 Descubrimiento profundo
> [Mensaje que apunta a uno de los 7 puntos clave: objetivo / dolor / miedo / deseo / situación / problema / costo de oportunidad. Pregunta que toca algo real de su vida. Listo para copiar.]

**OPCIÓN C** — 🌱 Siembra / Elevar nivel de conciencia
> [Mini historia real o reflexión que conecta con algo que el lead dijo. Activa curiosidad, identificación o imaginación. Rompe una creencia limitante de forma sutil. Si no hay suficiente base aún, hacé descubrimiento desde otro ángulo. Listo para copiar.]

---

🔄 **ESTRATEGIA ALTERNATIVA DE CANAL**
[SIEMPRE incluí esta sección. Basándote en el comportamiento específico de este lead (monosilábico, fantasma, activo, con objeciones, etc.), recomendá si conviene mantener el texto o cambiar de canal. Si hay señales de que el texto no funciona, proponé: audio de WhatsApp, videollamada corta, o conectar al lead con un alumno con perfil similar. Incluí exactamente QUÉ decirle para proponer ese cambio de canal, listo para copiar. Si el lead está respondiendo bien por texto, decí por qué seguir así y cuándo sería el momento de escalar el canal.]

---

🧠 **CÓMO MANEJAR ESTA SITUACIÓN**
[Si hay una objeción, duda o resistencia: explicá exactamente cómo abordarla con preguntas y acuerdos, nunca argumentando. Si hay apertura: cómo profundizarla. Si hay silencio o frío: cómo reactivar sin presionar.]

📋 **PLAN DE ACCIÓN — PRÓXIMOS 3 DÍAS**
- Hoy: [acción concreta]
- Mañana: [acción concreta — ej: seguimiento, pregunta específica, compartir algo del podcast]
- Pasado: [acción concreta]

❓ **PREGUNTA QUE LO PUEDE CAMBIAR TODO**
> [La pregunta más poderosa que {vendor['name']} le puede hacer a este lead en este momento. Una sola. Explicá en 1 línea por qué esta pregunta específicamente.]

⚠️ **ERROR A EVITAR AHORA MISMO**
[Una sola cosa — específica para esta situación — que si hace {vendor['name']} va a alejar al lead o cerrar la conversación. Con el motivo.]"""

        try:
            from google import genai as genai_client
            client = genai_client.Client(api_key=config.GEMINI_API_KEY)

            contents: list = [coach_prompt]
            if gemini_file_obj:
                contents = [gemini_file_obj, coach_prompt]

            response = client.models.generate_content(
                model=config.GEMINI_MODEL,
                contents=contents,
            )
            suggestion = response.text.strip()
        except Exception as e:
            logger.error("Gemini lanzamiento coach error: %s", e)
            return jsonify({"ok": False, "error": f"Error al generar sugerencias: {e}"}), 500
        finally:
            # Clean up temp file and Gemini file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
            if gemini_file_obj:
                try:
                    from google import genai as _gc
                    _gc.Client(api_key=config.GEMINI_API_KEY).files.delete(name=gemini_file_obj.name)
                except Exception:
                    pass

        query_text = user_text
        if media_file_desc:
            query_text = f"{user_text} [adjunto: {media_file_desc}]" if user_text else f"[adjunto: {media_file_desc}]"
        messages.append({"role": "vendor_query", "text": query_text})
        messages.append({"role": "coach_suggestion", "text": suggestion})
        database.lanzamiento_coach_update_messages(session_id, _json.dumps(messages))
        return jsonify({"ok": True, "suggestion": suggestion, "role": "coach"})


@app.route("/chat/lanzamiento/session/<int:session_id>/end", methods=["POST"])
def chat_lanzamiento_end(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False, "error": "No autenticado"}), 401

    sess = database.lanzamiento_coach_get(session_id)
    if not sess or sess["vendor_id"] != vendor["id"]:
        return jsonify({"ok": False, "error": "Sesión no válida"}), 403

    import google.generativeai as genai
    import json as _json

    messages = _json.loads(sess["messages_json"] or "[]")

    if sess["mode"] == "roleplay" and len(messages) >= 4:
        phase_info = LANZAMIENTO_PHASES.get(sess["phase"], LANZAMIENTO_PHASES["relacion"])
        conversation_text = "\n".join(
            f"{'VENDEDOR' if m['role'] == 'vendor' else 'LEAD'}: {m['text']}"
            for m in messages
        )
        feedback_prompt = f"""Sos un coach experto en ventas conversacionales de lanzamiento de Valentín Hernández (VH).
Acabás de observar este roleplay de práctica: {vendor['name']} practicó la etapa de {phase_info['label']} ({phase_info['days']}).
Lead: {sess['lead_name'] or 'Lead de práctica'}
Contexto: {sess['lead_context'] or 'Lead genérico del lanzamiento'}

OBJETIVO DE ESTA FASE: {phase_info['goal']}

CONVERSACIÓN:
{conversation_text}

Generá un feedback honesto, específico y accionable en español argentino:

## 🎯 Puntuación
[X/10 y 1 oración de síntesis]

## ✅ Lo que hiciste muy bien
[3-5 puntos concretos con citas de la conversación]

## ⚠️ Áreas a mejorar
[3-5 puntos con lo que dijo y lo que debería haber dicho]

## 💡 El momento clave que perdiste
[El momento exacto de la conversación donde había una oportunidad de oro y no se aprovechó — y cómo debería haberse manejado]

## 🔁 Para la próxima práctica
[2-3 acciones específicas para mejorar en el próximo roleplay de esta fase]

## 📊 Puntajes
relacion: [0-10]
descubrimiento: [0-10]
epp_formula: [0-10]
siembra: [0-10]
desapego: [0-10]
"""
        try:
            genai.configure(api_key=config.GEMINI_API_KEY)
            model = genai.GenerativeModel(config.GEMINI_MODEL)
            response = model.generate_content(feedback_prompt)
            feedback_text = response.text.strip()
        except Exception as e:
            logger.error("Gemini lanzamiento feedback error: %s", e)
            feedback_text = "No se pudo generar el feedback automáticamente."

        import re
        score = None
        sm = re.search(r"## 🎯 Puntuación\s*\n.*?(\d+(?:\.\d+)?)\s*/?\s*10", feedback_text)
        if sm:
            try:
                score = float(sm.group(1))
            except Exception:
                pass
        try:
            database.lanzamiento_coach_close(session_id, feedback_text, score)
        except Exception as e:
            logger.error("lanzamiento_coach_close error: %s", e)
            return jsonify({"ok": False, "error": f"Error al guardar sesión: {e}"}), 500
        return jsonify({"ok": True, "feedback": feedback_text, "score": score})
    else:
        try:
            database.lanzamiento_coach_close(session_id)
        except Exception as e:
            logger.error("lanzamiento_coach_close error: %s", e)
        return jsonify({"ok": True, "feedback": "", "score": None})


@app.route("/chat/lanzamiento/session/<int:session_id>/delete", methods=["POST"])
def chat_lanzamiento_delete_session(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return jsonify({"ok": False}), 401
    ok = database.delete_lanzamiento_session(session_id, vendor["id"])
    return jsonify({"ok": ok})


# ── Ventas Lanzamiento ─────────────────────────────────────────────────────────

@app.route("/ventas")
def ventas():
    redir = _require_producer()
    if redir:
        return redir
    from datetime import date as _date

    RAW = [
        {"n":1, "tipo_pago":"SEÑA",   "importe":150000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"19/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":2, "tipo_pago":"PAGO",   "importe":500000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"26/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":3, "tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Sofia Moyano",   "fecha":"25/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":4, "tipo_pago":"SEÑA",   "importe":475000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"26/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":5, "tipo_pago":"CUOTA 1","importe":750000,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"25/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":6, "tipo_pago":"PAGO",   "importe":1120000, "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"20/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":7, "tipo_pago":"PAGO",   "importe":1108890, "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"26/1/2026","cotizacion":1475,"metodo":"PESOS FINANCIERA"},
        {"n":8, "tipo_pago":"CUOTA 1","importe":150000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"31/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":9, "tipo_pago":"PAGO",   "importe":450000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"5/2/2026", "cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":10,"tipo_pago":"PAGO",   "importe":1120000, "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"13/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":11,"tipo_pago":"PAGO",   "importe":500,     "moneda":"USD","vendedor":"Celina Soto",    "fecha":"20/1/2026","cotizacion":None,"metodo":"Stripe"},
        {"n":12,"tipo_pago":"SEÑA",   "importe":220500,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"19/1/2026","cotizacion":1470,"metodo":"PESOS FINANCIERA"},
        {"n":13,"tipo_pago":"SEÑA",   "importe":150,     "moneda":"USD","vendedor":"Laura Asurmendi","fecha":"20/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":14,"tipo_pago":"PAGO",   "importe":293000,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"11/2/2026","cotizacion":1465,"metodo":"PESOS FINANCIERA"},
        {"n":15,"tipo_pago":"PAGO",   "importe":100000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"25/1/2026","cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":16,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"25/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":17,"tipo_pago":"PAGO",   "importe":720000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"4/2/2026", "cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":18,"tipo_pago":"SEÑA",   "importe":400000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"14/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":19,"tipo_pago":"CUOTA 1","importe":500000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"19/2/2026","cotizacion":1440,"metodo":"PESOS FINANCIERA"},
        {"n":20,"tipo_pago":"SEÑA",   "importe":100000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"26/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":21,"tipo_pago":"CUOTA 2","importe":150000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"10/3/2026","cotizacion":1425,"metodo":"PESOS FINANCIERA"},
        {"n":22,"tipo_pago":"PAGO",   "importe":1125000, "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"20/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":23,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Gianina Yelme",  "fecha":"25/1/2026","cotizacion":1500,"metodo":"BINANCE"},
        {"n":24,"tipo_pago":"SEÑA",   "importe":375000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"24/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":25,"tipo_pago":"PAGO",   "importe":357500,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"20/2/2026","cotizacion":1430,"metodo":"PESOS FINANCIERA"},
        {"n":26,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Hugo Loncaric",  "fecha":"21/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":27,"tipo_pago":"SEÑA",   "importe":100000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"24/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":28,"tipo_pago":"SEÑA",   "importe":150000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"24/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":29,"tipo_pago":"PAGO",   "importe":1110000, "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"25/1/2026","cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":30,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Gianina Yelme",  "fecha":"22/1/2026","cotizacion":None,"metodo":"BINANCE"},
        {"n":31,"tipo_pago":"SEÑA",   "importe":288000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"21/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":32,"tipo_pago":"CUOTA 1","importe":100000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"23/2/2026","cotizacion":1430,"metodo":"PESOS FINANCIERA"},
        {"n":33,"tipo_pago":"SEÑA",   "importe":150000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"1/2/2026", "cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":34,"tipo_pago":"CUOTA 1","importe":745000,  "moneda":"ARS","vendedor":"Roque Herrera",  "fecha":"20/1/2026","cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":35,"tipo_pago":"SEÑA",   "importe":150500,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"24/1/2026","cotizacion":1505,"metodo":"PESOS FINANCIERA"},
        {"n":36,"tipo_pago":"CUOTA 1","importe":592000,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"5/2/2026", "cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":37,"tipo_pago":"SEÑA",   "importe":200000,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"25/1/2026","cotizacion":1470,"metodo":"PESOS FINANCIERA"},
        {"n":38,"tipo_pago":"PAGO",   "importe":550000,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"6/2/2026", "cotizacion":1470,"metodo":"PESOS FINANCIERA"},
        {"n":39,"tipo_pago":"SEÑA",   "importe":100000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"26/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":40,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Sofia Moyano",   "fecha":"25/1/2026","cotizacion":None,"metodo":"Stripe"},
        {"n":41,"tipo_pago":"CUOTA 1","importe":250,     "moneda":"USD","vendedor":"Ramiro Ledesma", "fecha":"6/2/2026", "cotizacion":1480,"metodo":"Stripe"},
        {"n":42,"tipo_pago":"CUOTA 1","importe":250,     "moneda":"USD","vendedor":"Ramiro Ledesma", "fecha":"12/3/2026","cotizacion":1415,"metodo":"Stripe"},
        {"n":43,"tipo_pago":"CUOTA 2","importe":250,     "moneda":"USD","vendedor":"Ramiro Ledesma", "fecha":"26/3/2026","cotizacion":None,"metodo":"Stripe"},
        {"n":44,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"19/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":45,"tipo_pago":"SEÑA",   "importe":100000,  "moneda":"ARS","vendedor":"Roque Herrera",  "fecha":"29/1/2026","cotizacion":1475,"metodo":"PESOS FINANCIERA"},
        {"n":46,"tipo_pago":"PAGO",   "importe":745000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"20/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":47,"tipo_pago":"CUOTA 1","importe":500,     "moneda":"USD","vendedor":"Sofia Moyano",   "fecha":"20/1/2026","cotizacion":None,"metodo":"Stripe"},
        {"n":48,"tipo_pago":"SEÑA",   "importe":275000,  "moneda":"ARS","vendedor":"Roque Herrera",  "fecha":"2/2/2026", "cotizacion":1475,"metodo":"PESOS FINANCIERA"},
        {"n":49,"tipo_pago":"CUOTA 1","importe":140000,  "moneda":"ARS","vendedor":"Roque Herrera",  "fecha":"16/2/2026","cotizacion":1400,"metodo":"PESOS FINANCIERA"},
        {"n":50,"tipo_pago":"CUOTA 2","importe":284000,  "moneda":"ARS","vendedor":"Roque Herrera",  "fecha":"2/3/2026", "cotizacion":1420,"metodo":"PESOS FINANCIERA"},
        {"n":51,"tipo_pago":"SEÑA",   "importe":145000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"19/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":52,"tipo_pago":"SEÑA",   "importe":370000,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"9/2/2026", "cotizacion":1435,"metodo":"PESOS FINANCIERA"},
        {"n":53,"tipo_pago":"PAGO",   "importe":706250,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"9/2/2026", "cotizacion":1435,"metodo":"PESOS FINANCIERA"},
        {"n":54,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Gianina Yelme",  "fecha":"29/1/2026","cotizacion":1470,"metodo":"DOLARES MULTID"},
        {"n":55,"tipo_pago":"SEÑA",   "importe":100,     "moneda":"USD","vendedor":"Roque Herrera",  "fecha":"22/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":56,"tipo_pago":"PAGO",   "importe":430,     "moneda":"USD","vendedor":"Roque Herrera",  "fecha":"26/1/2026","cotizacion":1475,"metodo":"DOLARES MULTID"},
        {"n":57,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Gianina Yelme",  "fecha":"22/1/2026","cotizacion":None,"metodo":"BINANCE"},
        {"n":58,"tipo_pago":"SEÑA",   "importe":560000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"20/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":59,"tipo_pago":"SEÑA",   "importe":185000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"22/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":60,"tipo_pago":"SEÑA",   "importe":148500,  "moneda":"ARS","vendedor":"Roque Herrera",  "fecha":"25/1/2026","cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":61,"tipo_pago":"SEÑA",   "importe":372000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"19/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":62,"tipo_pago":"PAGO",   "importe":355000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"24/2/2026","cotizacion":1420,"metodo":"PESOS FINANCIERA"},
        {"n":63,"tipo_pago":"SEÑA",   "importe":400000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"27/1/2026","cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":64,"tipo_pago":"SEÑA",   "importe":294000,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"25/1/2026","cotizacion":1470,"metodo":"PESOS FINANCIERA"},
        {"n":65,"tipo_pago":"PAGO",   "importe":530000,  "moneda":"USD","vendedor":"Tomas Garcia",   "fecha":"19/3/2026","cotizacion":1435,"metodo":"PESOS MULTI D"},
        {"n":66,"tipo_pago":"SEÑA",   "importe":367500,  "moneda":"ARS","vendedor":"Laura Asurmendi","fecha":"19/1/2026","cotizacion":1470,"metodo":"PESOS FINANCIERA"},
        {"n":67,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"26/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":68,"tipo_pago":"PAGO",   "importe":270000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"26/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":69,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"19/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":70,"tipo_pago":"SEÑA",   "importe":250000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"19/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":71,"tipo_pago":"SEÑA",   "importe":500000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"14/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":72,"tipo_pago":"SEÑA",   "importe":326000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"22/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":73,"tipo_pago":"PAGO",   "importe":742500,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"24/1/2026","cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":74,"tipo_pago":"PAGO",   "importe":248850,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"27/3/2026","cotizacion":1382,"metodo":"PESOS FINANCIERA"},
        {"n":75,"tipo_pago":"SEÑA",   "importe":200,     "moneda":"USD","vendedor":"Sofia Moyano",   "fecha":"28/1/2026","cotizacion":1475,"metodo":"DOLARES MULTID"},
        {"n":76,"tipo_pago":"CUOTA 2","importe":169800,  "moneda":"ARS","vendedor":"Sofia Moyano",   "fecha":"4/3/2026", "cotizacion":1415,"metodo":"PESOS FINANCIERA"},
        {"n":77,"tipo_pago":"SEÑA",   "importe":150000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"28/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":78,"tipo_pago":"PAGO",   "importe":400,     "moneda":"USD","vendedor":"Ramiro Ledesma", "fecha":"5/2/2026", "cotizacion":None,"metodo":"Stripe"},
        {"n":79,"tipo_pago":"PAGO",   "importe":592000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"30/1/2026","cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":80,"tipo_pago":"SEÑA",   "importe":100,     "moneda":"USD","vendedor":"Hugo Loncaric",  "fecha":"19/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":81,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Gianina Yelme",  "fecha":"20/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":82,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Gianina Yelme",  "fecha":"12/1/2026","cotizacion":1505,"metodo":"DOLARES MULTID"},
        {"n":83,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"19/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":84,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"18/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":85,"tipo_pago":"PAGO",   "importe":750,     "moneda":"USD","vendedor":"Sofia Moyano",   "fecha":"21/1/2026","cotizacion":None,"metodo":"DOLARES MULTID"},
        {"n":86,"tipo_pago":"CUOTA 1","importe":375000,  "moneda":"ARS","vendedor":"Tomas Garcia",   "fecha":"4/2/2026", "cotizacion":1455,"metodo":"PESOS FINANCIERA"},
        {"n":87,"tipo_pago":"CUOTA 2","importe":375000,  "moneda":"ARS","vendedor":"Tomas Garcia",   "fecha":"2/3/2026", "cotizacion":1440,"metodo":"PESOS FINANCIERA"},
        {"n":88,"tipo_pago":"PAGO",   "importe":750000,  "moneda":"ARS","vendedor":"Gianina Yelme",  "fecha":"19/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":89,"tipo_pago":"PAGO",   "importe":747000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"2/2/2026", "cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":90,"tipo_pago":"SEÑA",   "importe":372500,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"22/1/2026","cotizacion":1490,"metodo":"PESOS FINANCIERA"},
        {"n":91,"tipo_pago":"PAGO",   "importe":340000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"15/2/2026","cotizacion":1440,"metodo":"PESOS FINANCIERA"},
        {"n":92,"tipo_pago":"PAGO",   "importe":100000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"26/1/2026","cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":93,"tipo_pago":"PAGO",   "importe":150000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"13/2/2026","cotizacion":1440,"metodo":"PESOS FINANCIERA"},
        {"n":94,"tipo_pago":"SEÑA",   "importe":360000,  "moneda":"ARS","vendedor":"Ramiro Ledesma", "fecha":"19/1/2026","cotizacion":1500,"metodo":"PESOS FINANCIERA"},
        {"n":95,"tipo_pago":"SEÑA",   "importe":100000,  "moneda":"ARS","vendedor":"Hugo Loncaric",  "fecha":"25/1/2026","cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":96,"tipo_pago":"PAGO",   "importe":400000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"24/1/2026","cotizacion":1480,"metodo":"PESOS FINANCIERA"},
        {"n":97,"tipo_pago":"PAGO",   "importe":100000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"25/1/2026","cotizacion":1485,"metodo":"PESOS FINANCIERA"},
        {"n":98,"tipo_pago":"PAGO",   "importe":650000,  "moneda":"ARS","vendedor":"Celina Soto",    "fecha":"16/2/2026","cotizacion":1440,"metodo":"PESOS FINANCIERA"},
    ]

    from collections import defaultdict
    from datetime import datetime as _dt

    AVG_COTIZACION = 1476  # global average from PDF

    def parse_fecha(s):
        d, m, y = s.split("/")
        return _date(int(y), int(m), int(d))

    # ── Email mapping (deduplication key) ──────────────────────────────────────
    # Row 65: importe 530000 marcado como USD pero metodo de pago es "PESOS MULTI D"
    # y tiene cotizacion → se corrige a ARS
    EMAILS = {
        1:"alexiaguraiib.nm@gmail.com",2:"roldan.debb@gmail.com",3:"lalibaez75@gmail.com",
        4:"villamarc@hotmail.com",5:"agustinachazarreta72@gmail.com",6:"alearredondofrontera@gmail.com",
        7:"aleddl69@gmail.com",8:"alexiaguraiib.nm@gmail.com",9:"alexiaguraiib.nm@gmail.com",
        10:"aarano_99@yahoo.com",11:"anibal_silva29@hotmail.com",12:"ardimalopez@gmail.com",
        13:"ardimalopez@gmail.com",14:"ardimalopez@gmail.com",15:"axelkevin207@gmail.com",
        16:"orcelletb@gmail.com",17:"cintiacarry@gmail.com",18:"cintiacarry@gmail.com",
        19:"caccossatto@gmail.com",20:"caccossatto@gmail.com",21:"caccossatto@gmail.com",
        22:"rfdaiana@gmail.com",23:"dali2025dali@gmail.com",24:"dulcerecreodulce@gmail.com",
        25:"dulcerecreodulce@gmail.com",26:"danisalleszu01@gmail.com",27:"roldan.debb@gmail.com",
        28:"roldan.debb@gmail.com",29:"cramacagnodebora@gmail.com",30:"djoaquinmoreira@gmail.com",
        31:"enzoacastro2025@gmail.com",32:"efabio@agro.unc.edu.ar",33:"efabio@agro.unc.edu.ar",
        34:"gonzapintos.cas.12@gmail.com",35:"grisluz750@gmail.com",36:"grisluz750@gmail.com",
        37:"guillermodaniel91@gmail.com",38:"guillermodaniel91@gmail.com",
        39:"cardozomaximiliano@gmail.com",40:"fernandez.isoledad@gmail.com",
        41:"ivana.carrizo1482@gmail.com",42:"ivana.carrizo1482@gmail.com",
        43:"ivana.carrizo1482@gmail.com",44:"fojoivana@gmail.com",
        45:"joaquinduhalde18@gmail.com",46:"alessiojorge@hotmail.com",
        47:"hernan_reybas@hotmail.com",48:"landerjuan19@gmail.com",
        49:"landerjuan19@gmail.com",50:"landerjuan19@gmail.com",
        51:"alvarezcaggianojuanmanuel@gmail.com",52:"jikijikivane@gmail.com",
        53:"jikijikivane@gmail.com",54:"laura0103gr@yahoo.com.ar",
        55:"laubaudracco@gmail.com",56:"laubaudracco@gmail.com",
        57:"leeilayazmin@gmail.com",58:"leonardogrivero209@gmail.com",
        59:"leonardogrivero@gmail.com",60:"lihuemontenegrolab@gmail.com",
        61:"lorenataparello@gmail.com",62:"lorenataparello@gmail.com",
        63:"lucasandresnozica@gmail.com",64:"luuciamgomez@gmail.com",
        65:"luuciamgomez@gmail.com",66:"maca.-1402@hotmail.com",
        67:"manujaime@hotmail.com",68:"villamarc@hotmail.com",
        69:"mariae-r@hotmail.com",70:"inesformento@gmail.com",
        71:"inesformento@gmail.com",72:"inesformento@gmail.com",
        73:"marodb4@gmail.com",74:"vanesapelayes1@gmail.com",
        75:"vanesapelayes1@gmail.com",76:"vanesapelayes1@gmail.com",
        77:"lic.marielarodriguez91@gmail.com",78:"lic.marielarodriguez91@gmail.com",
        79:"mely.cassero@gmail.com",80:"mely.cassero@gmail.com",
        81:"estarbienpnl@gmail.com",82:"nadyam.deliberto@gmail.com",
        83:"leire-07@hotmail.com",84:"gpaolag76@gmail.com",
        85:"elianapere17@gmail.com",86:"rolopez.03@gmail.com",
        87:"rolopez.03@gmail.com",88:"roxi_molina@yahoo.com",
        89:"roxanafares67@gmail.com",90:"roxanafares67@gmail.com",
        91:"edusnaider7@gmail.com",92:"santzamunda@gmail.com",
        93:"santzamunda@gmail.com",94:"saragarro72@gmail.com",
        95:"sildobosz@hotmail.com",96:"edusnaider7@gmail.com",
        97:"qves86@gmail.com",98:"qves86@gmail.com",
    }

    def to_usd(r):
        # Row 65 está marcado USD pero el monto (530000) y el método ("PESOS MULTI D")
        # indican que es ARS → se trata como ARS
        moneda = r["moneda"]
        if r["n"] == 65:
            moneda = "ARS"
        if moneda == "USD":
            return r["importe"]
        cot = r["cotizacion"] or AVG_COTIZACION
        return round(r["importe"] / cot, 2)

    # Enrich records
    for r in RAW:
        r["cliente_email"] = EMAILS.get(r["n"], "")
        r["fecha_obj"] = parse_fecha(r["fecha"])
        r["usd_equiv"] = to_usd(r)
        d = r["fecha_obj"]
        if d < _date(2026, 1, 11):
            r["semana"] = "Pre-lanzamiento"
        elif d <= _date(2026, 1, 17):
            r["semana"] = "Semana 1 (11-17 Ene)"
        elif d <= _date(2026, 1, 24):
            r["semana"] = "Semana 2 (18-24 Ene)"
        elif d <= _date(2026, 1, 31):
            r["semana"] = "Semana 3 (25-31 Ene)"
        else:
            r["semana"] = "Post-lanzamiento"

    # Mark first payment per client (= the actual sale)
    seen_emails = set()
    for r in sorted(RAW, key=lambda x: (x["fecha_obj"], x["n"])):
        email = r["cliente_email"]
        if email not in seen_emails:
            r["is_first_payment"] = True
            seen_emails.add(email)
        else:
            r["is_first_payment"] = False

    first_payments = [r for r in RAW if r["is_first_payment"]]
    total_sales = len(first_payments)  # 65 ventas únicas

    # 1. Ventas únicas por día (solo first payments) + revenue total por día
    daily_sales = defaultdict(int)   # unique sales (clients)
    daily_usd = defaultdict(float)   # all revenue
    for r in RAW:
        if r["is_first_payment"]:
            daily_sales[r["fecha"]] += 1
        daily_usd[r["fecha"]] += r["usd_equiv"]

    sorted_days = sorted(daily_sales.keys(), key=parse_fecha)
    daily_labels = sorted_days
    daily_count_vals = [daily_sales[d] for d in sorted_days]
    daily_usd_vals = [round(daily_usd[d], 0) for d in sorted_days]

    # 2. Días top (por ventas únicas)
    top_days = sorted(daily_sales.items(), key=lambda x: -x[1])[:8]

    # 3. Semanas — ventas únicas + revenue
    SEMANA_ORDER = ["Pre-lanzamiento", "Semana 1 (11-17 Ene)", "Semana 2 (18-24 Ene)",
                    "Semana 3 (25-31 Ene)", "Post-lanzamiento"]
    semana_sales = defaultdict(int)
    semana_usd = defaultdict(float)
    for r in RAW:
        if r["is_first_payment"]:
            semana_sales[r["semana"]] += 1
        semana_usd[r["semana"]] += r["usd_equiv"]
    semana_count_vals = [semana_sales[s] for s in SEMANA_ORDER]
    semana_usd_vals = [round(semana_usd[s], 0) for s in SEMANA_ORDER]

    # 4. Ticket promedio — basado en ventas únicas (primer pago como referencia del ticket)
    ars_first = [r for r in first_payments if r["moneda"] == "ARS" or r["n"] == 65]
    usd_first = [r for r in first_payments if r["moneda"] == "USD" and r["n"] != 65]
    # Total revenue por moneda (todos los pagos)
    total_ars = sum(r["importe"] for r in RAW if r["moneda"] == "ARS" or r["n"] == 65)
    total_usd_direct = sum(r["importe"] for r in RAW if r["moneda"] == "USD" and r["n"] != 65)
    avg_ticket_ars = round(total_ars / len(ars_first)) if ars_first else 0
    avg_ticket_usd = round(total_usd_direct / len(usd_first), 0) if usd_first else 0
    n_ars = len([r for r in RAW if r["moneda"] == "ARS" or r["n"] == 65])
    n_usd = len([r for r in RAW if r["moneda"] == "USD" and r["n"] != 65])

    # 5. Ranking vendedores: ventas únicas cerradas + ingresos totales generados
    vendor_sales_count = defaultdict(int)
    vendor_usd = defaultdict(float)
    for r in RAW:
        if r["is_first_payment"]:
            vendor_sales_count[r["vendedor"]] += 1
        vendor_usd[r["vendedor"]] += r["usd_equiv"]
    # Sort by revenue (USD equiv)
    vendor_ranking = sorted(vendor_usd.items(), key=lambda x: -x[1])
    vendor_full_labels = [v[0] for v in vendor_ranking]
    vendor_usd_vals = [round(v[1], 0) for v in vendor_ranking]
    vendor_sales_vals = [vendor_sales_count[v[0]] for v in vendor_ranking]

    # 6. Global KPIs
    total_usd_equiv = round(sum(r["usd_equiv"] for r in RAW), 0)
    top_vendor = vendor_ranking[0][0] if vendor_ranking else ""
    top_vendor_usd = vendor_ranking[0][1] if vendor_ranking else 0
    top_vendor_sales = vendor_sales_count.get(top_vendor, 0)

    # 7. Cash collected — aplicando fees por billetera
    # fee_pct: porcentaje que descuenta la plataforma (0.07 = 7%)
    FEE = {
        "PESOS FINANCIERA": 0.07,
        "DOLARES MULTID":   0.21,
        "PESOS MULTI D":    0.21,
        "BINANCE":          0.00,
        "Stripe":           0.058,
        "PAYPAL":           0.09,
    }
    def get_fee(metodo):
        return FEE.get(metodo.strip(), 0.0)

    # Acumular por billetera: bruto, fee_amount, neto — separado ARS vs USD
    from collections import OrderedDict
    wallet_stats = {}   # key: metodo → {bruto_ars, bruto_usd, fee_ars, fee_usd, neto_ars, neto_usd, count}
    for r in RAW:
        m = r["metodo"]
        moneda = "ARS" if (r["moneda"] == "ARS" or r["n"] == 65) else "USD"
        bruto = r["importe"]
        fee_pct = get_fee(m)
        fee_amt = round(bruto * fee_pct, 2)
        neto = round(bruto - fee_amt, 2)
        if m not in wallet_stats:
            wallet_stats[m] = {"bruto_ars":0,"bruto_usd":0,"fee_ars":0,"fee_usd":0,"neto_ars":0,"neto_usd":0,"count":0,"fee_pct":fee_pct}
        wallet_stats[m]["count"] += 1
        if moneda == "ARS":
            wallet_stats[m]["bruto_ars"] += bruto
            wallet_stats[m]["fee_ars"]   += fee_amt
            wallet_stats[m]["neto_ars"]  += neto
        else:
            wallet_stats[m]["bruto_usd"] += bruto
            wallet_stats[m]["fee_usd"]   += fee_amt
            wallet_stats[m]["neto_usd"]  += neto

    # Totales ARS
    cash_bruto_ars  = sum(v["bruto_ars"] for v in wallet_stats.values())
    cash_fee_ars    = sum(v["fee_ars"]   for v in wallet_stats.values())
    cash_neto_ars   = sum(v["neto_ars"]  for v in wallet_stats.values())
    # Totales USD
    cash_bruto_usd  = sum(v["bruto_usd"] for v in wallet_stats.values())
    cash_fee_usd    = sum(v["fee_usd"]   for v in wallet_stats.values())
    cash_neto_usd   = sum(v["neto_usd"]  for v in wallet_stats.values())
    # Unificado en USD equiv (usando cotizacion promedio para ARS)
    unified_bruto_usd = round(cash_bruto_ars / AVG_COTIZACION + cash_bruto_usd, 2)
    unified_fee_usd   = round(cash_fee_ars   / AVG_COTIZACION + cash_fee_usd,   2)
    unified_neto_usd  = round(cash_neto_ars  / AVG_COTIZACION + cash_neto_usd,  2)
    # Unificado en ARS equiv
    unified_bruto_ars_eq = round(cash_bruto_ars + cash_bruto_usd * AVG_COTIZACION, 0)
    unified_fee_ars_eq   = round(cash_fee_ars   + cash_fee_usd   * AVG_COTIZACION, 0)
    unified_neto_ars_eq  = round(cash_neto_ars  + cash_neto_usd  * AVG_COTIZACION, 0)

    # Ordenar wallets por bruto total (ARS equiv) desc
    wallet_list = sorted(
        [{"metodo": k, **v} for k, v in wallet_stats.items()],
        key=lambda x: -(x["bruto_ars"] + x["bruto_usd"] * AVG_COTIZACION)
    )

    # Cash neto por vendedor (para gráfico de ingresos reales)
    vendor_neto_usd = defaultdict(float)
    for r in RAW:
        m = r["metodo"]
        moneda = "ARS" if (r["moneda"] == "ARS" or r["n"] == 65) else "USD"
        bruto = r["importe"]
        fee_pct = get_fee(m)
        neto = bruto * (1 - fee_pct)
        if moneda == "ARS":
            neto_usd = neto / (r["cotizacion"] or AVG_COTIZACION)
        else:
            neto_usd = neto
        vendor_neto_usd[r["vendedor"]] += neto_usd
    # Mismo orden que vendor_ranking (por bruto)
    vendor_neto_vals = [round(vendor_neto_usd[v], 0) for v in vendor_full_labels]

    # Tipo de pago distribution (for donut)
    tipo_counts = defaultdict(int)
    tipo_ars = defaultdict(float)
    for r in RAW:
        tipo_counts[r["tipo_pago"]] += 1
        moneda = "ARS" if (r["moneda"] == "ARS" or r["n"] == 65) else "USD"
        usd_val = r["importe"] if moneda == "USD" else r["importe"] / (r["cotizacion"] or AVG_COTIZACION)
        tipo_ars[r["tipo_pago"]] += usd_val
    tipo_labels_list  = sorted(tipo_counts.keys())
    tipo_counts_list  = [tipo_counts[t] for t in tipo_labels_list]
    tipo_usd_list     = [round(tipo_ars[t], 0) for t in tipo_labels_list]

    # 8. Table data — sort by fecha, mark duplicates
    table_rows = sorted(RAW, key=lambda r: (r["fecha_obj"], r["n"]))
    for r in table_rows:
        del r["fecha_obj"]

    # ── Lanzamiento 5 data ──────────────────────────────────────────────────
    L5_AVG_COT = 1150  # cotización promedio estimada; actualizable
    l5_raw = database.l5_get_all()

    def l5_to_usd(r):
        if r["moneda"] == "USD":
            return r["importe"]
        return round(r["importe"] / (r["cotizacion"] or L5_AVG_COT), 2)

    def l5_parse_fecha(s):
        try:
            parts = s.split("/")
            if len(parts) == 3:
                return _date(int(parts[2]), int(parts[1]), int(parts[0]))
        except Exception:
            pass
        return _date(2026, 1, 1)

    seen_l5 = set()
    for r in sorted(l5_raw, key=lambda x: (l5_parse_fecha(x["fecha"]), x["id"])):
        ref = r["cliente_ref"].strip().lower() if r["cliente_ref"] else f"__noid_{r['id']}"
        if ref not in seen_l5:
            r["is_first"] = True
            seen_l5.add(ref)
        else:
            r["is_first"] = False
        r["usd_equiv"] = l5_to_usd(r)

    l5_ventas_unicas = sum(1 for r in l5_raw if r.get("is_first"))
    l5_total_usd = round(sum(r["usd_equiv"] for r in l5_raw), 2)
    l5_total_ars = sum(r["importe"] for r in l5_raw if r["moneda"] == "ARS")
    l5_total_usd_direct = sum(r["importe"] for r in l5_raw if r["moneda"] == "USD")

    # Daily sales for L5
    l5_daily = defaultdict(lambda: {"count": 0, "usd": 0.0})
    for r in l5_raw:
        if r.get("is_first"):
            l5_daily[r["fecha"]]["count"] += 1
        l5_daily[r["fecha"]]["usd"] += r["usd_equiv"]
    l5_sorted_days = sorted(l5_daily.keys(), key=l5_parse_fecha)

    # Per-vendor totals for L5
    l5_vendor_sales = defaultdict(int)
    l5_vendor_usd = defaultdict(float)
    for r in l5_raw:
        if r.get("is_first"):
            l5_vendor_sales[r["vendedor"]] += 1
        l5_vendor_usd[r["vendedor"]] += r["usd_equiv"]
    l5_vendor_ranking = sorted(l5_vendor_usd.items(), key=lambda x: -x[1])

    # Cash collected L5
    l5_neto_usd = 0.0
    l5_neto_ars = 0.0
    for r in l5_raw:
        fee = FEE.get(r["metodo"].strip(), 0.0)
        neto = r["importe"] * (1 - fee)
        if r["moneda"] == "USD":
            l5_neto_usd += neto
        else:
            l5_neto_ars += neto

    return render_template(
        "ventas.html",
        total_sales=total_sales,
        total_transactions=len(RAW),
        total_usd_equiv=int(total_usd_equiv),
        total_ars=total_ars,
        total_usd_direct=int(total_usd_direct),
        avg_ticket_ars=avg_ticket_ars,
        avg_ticket_usd=int(avg_ticket_usd),
        top_vendor=top_vendor,
        top_vendor_usd=int(top_vendor_usd),
        top_vendor_sales=top_vendor_sales,
        daily_labels=json.dumps(daily_labels),
        daily_count_vals=json.dumps(daily_count_vals),
        daily_usd_vals=json.dumps(daily_usd_vals),
        semana_labels=json.dumps(SEMANA_ORDER),
        semana_count_vals=json.dumps(semana_count_vals),
        semana_usd_vals=json.dumps(semana_usd_vals),
        vendor_full_labels=json.dumps(vendor_full_labels),
        vendor_usd_vals=json.dumps(vendor_usd_vals),
        vendor_sales_vals=json.dumps(vendor_sales_vals),
        vendor_neto_vals=json.dumps(vendor_neto_vals),
        top_days=top_days,
        table_rows=table_rows,
        n_ars=n_ars,
        n_usd=n_usd,
        # cash collected
        wallet_list=wallet_list,
        cash_bruto_ars=int(cash_bruto_ars),
        cash_fee_ars=int(cash_fee_ars),
        cash_neto_ars=int(cash_neto_ars),
        cash_bruto_usd=round(cash_bruto_usd, 2),
        cash_fee_usd=round(cash_fee_usd, 2),
        cash_neto_usd=round(cash_neto_usd, 2),
        unified_bruto_usd=round(unified_bruto_usd, 0),
        unified_fee_usd=round(unified_fee_usd, 0),
        unified_neto_usd=round(unified_neto_usd, 0),
        unified_bruto_ars_eq=int(unified_bruto_ars_eq),
        unified_fee_ars_eq=int(unified_fee_ars_eq),
        unified_neto_ars_eq=int(unified_neto_ars_eq),
        avg_cotizacion=AVG_COTIZACION,
        tipo_labels=json.dumps(tipo_labels_list),
        tipo_counts=json.dumps(tipo_counts_list),
        tipo_usd=json.dumps(tipo_usd_list),
        # Lanzamiento 5
        l5_raw=l5_raw,
        l5_ventas_unicas=l5_ventas_unicas,
        l5_total_usd=l5_total_usd,
        l5_total_ars=int(l5_total_ars),
        l5_total_usd_direct=round(l5_total_usd_direct, 2),
        l5_neto_ars=int(l5_neto_ars),
        l5_neto_usd=round(l5_neto_usd, 2),
        l5_sorted_days=l5_sorted_days,
        l5_daily=dict(l5_daily),
        l5_vendor_ranking=l5_vendor_ranking,
        l5_vendor_sales=dict(l5_vendor_sales),
        l5_avg_cot=L5_AVG_COT,
    )


# ── Lanzamiento 5 ventas API ──────────────────────────────────────────────────

@app.route("/ventas/l5/add", methods=["POST"])
def ventas_l5_add():
    redir = _require_producer()
    if redir: return jsonify({"ok": False}), 403
    d = request.get_json() or {}
    try:
        entry_id = database.l5_add_venta(
            tipo_pago=(d.get("tipo_pago") or "PAGO").strip(),
            importe=float(d.get("importe") or 0),
            moneda=(d.get("moneda") or "ARS").strip().upper(),
            vendedor=(d.get("vendedor") or "").strip(),
            fecha=(d.get("fecha") or "").strip(),
            cotizacion=float(d["cotizacion"]) if d.get("cotizacion") else None,
            metodo=(d.get("metodo") or "PESOS FINANCIERA").strip(),
            cliente_ref=(d.get("cliente_ref") or "").strip(),
        )
        return jsonify({"ok": True, "id": entry_id})
    except Exception as e:
        logger.error("l5 add error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/ventas/l5/delete/<int:entry_id>", methods=["POST"])
def ventas_l5_delete(entry_id: int):
    redir = _require_producer()
    if redir: return jsonify({"ok": False}), 403
    ok = database.l5_delete_venta(entry_id)
    return jsonify({"ok": ok})


# ── Lanzamiento feedback ──────────────────────────────────────────────────────

@app.route("/lanzamiento/kpi")
def lanzamiento_kpi():
    redir = _require_vendor()
    if redir: return redir
    vendor_id = request.args.get("vendor_id", type=int)
    saved = request.args.get("saved", "")
    vendors_list = database.get_kpi_vendors()
    today = datetime.now(TZ).date().isoformat()

    if not vendor_id:
        return render_template("lanzamiento_kpi.html",
                               vendors_list=vendors_list, vendor=None,
                               today=today, today_entry={},
                               entries=[], totals={}, saved="")

    vendor = next((v for v in vendors_list if v["id"] == vendor_id), None)
    if not vendor:
        return redirect(url_for("lanzamiento_kpi"))

    today_entry = database.kpi_get_entry(vendor_id, today) or {}
    entries = database.kpi_get_vendor_entries(vendor_id, days=90)
    totals = database.kpi_aggregate_entries(entries)
    goals = database.kpi_get_vendor_goals(vendor_id)
    custom_labels = database.kpi_get_active_labels()
    all_labels = database.kpi_get_all_labels()
    entry_date_param = request.args.get("entry_date", today)
    saved_today = bool(saved) and entry_date_param == today
    is_producer = bool(flask_session.get("producer_auth"))
    return render_template("lanzamiento_kpi.html",
                           vendors_list=vendors_list, vendor=vendor,
                           today=today, today_entry=today_entry,
                           entries=entries, totals=totals, saved=saved,
                           saved_today=saved_today, goals=goals,
                           custom_labels=custom_labels, all_labels=all_labels,
                           is_producer=is_producer)


@app.route("/lanzamiento/kpi/save", methods=["POST"])
def lanzamiento_kpi_save():
    data = request.get_json()
    vendor_id = data.get("vendor_id")
    if not vendor_id:
        return jsonify({"ok": False, "error": "Falta el vendedor"}), 400
    entry_date = (data.get("entry_date") or "").strip()
    if not entry_date:
        entry_date = datetime.now(TZ).date().isoformat()
    database.kpi_upsert_entry(int(vendor_id), entry_date, data)
    redirect_url = url_for("lanzamiento_kpi", vendor_id=vendor_id, saved=1)
    return jsonify({"ok": True, "redirect": redirect_url})


@app.route("/lanzamiento/kpi/history")
def lanzamiento_kpi_history():
    vendor_id = request.args.get("vendor_id", type=int)
    if not vendor_id:
        return jsonify({"ok": False, "error": "Falta vendor_id"}), 400
    days = int(request.args.get("days", 90))
    entries = database.kpi_get_vendor_entries(vendor_id, days=days)
    return jsonify({"ok": True, "entries": entries})


@app.route("/lanzamiento/kpi/delete", methods=["POST"])
def lanzamiento_kpi_delete():
    data = request.get_json()
    vendor_id = data.get("vendor_id")
    entry_dates = data.get("entry_dates", [])
    if not vendor_id or not entry_dates:
        return jsonify({"ok": False, "error": "Faltan datos"}), 400
    deleted = database.kpi_delete_entries(vendor_id, entry_dates)
    return jsonify({"ok": True, "deleted": deleted})


@app.route("/lanzamiento/kpi/goals", methods=["POST"])
def lanzamiento_kpi_goals():
    data = request.get_json()
    vendor_id = data.get("vendor_id")
    if not vendor_id:
        return jsonify({"ok": False, "error": "Falta el vendedor"}), 400
    database.kpi_save_vendor_goals(
        int(vendor_id),
        int(data.get("goal_ventas", 0) or 0),
        int(data.get("goal_potencial", 0) or 0),
        int(data.get("goal_conv_fluida", 0) or 0),
    )
    return jsonify({"ok": True})


@app.route("/lanzamiento/kpi/labels/add", methods=["POST"])
def lanzamiento_kpi_labels_add():
    redir = _require_producer()
    if redir: return redir
    data = request.get_json()
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Nombre requerido"}), 400
    label_id = database.kpi_add_label(name)
    return jsonify({"ok": True, "id": label_id})


@app.route("/lanzamiento/kpi/labels/toggle", methods=["POST"])
def lanzamiento_kpi_labels_toggle():
    redir = _require_producer()
    if redir: return redir
    data = request.get_json()
    database.kpi_toggle_label(int(data["id"]), int(data["active"]))
    return jsonify({"ok": True})


@app.route("/lanzamiento/kpi/labels/delete", methods=["POST"])
def lanzamiento_kpi_labels_delete():
    redir = _require_producer()
    if redir: return redir
    data = request.get_json()
    database.kpi_delete_label(int(data["id"]))
    return jsonify({"ok": True})


@app.route("/lanzamiento/kpi/strategy", methods=["POST"])
def lanzamiento_kpi_strategy():
    # No auth required — open endpoint for vendors and director
    import google.generativeai as genai

    data = request.get_json()
    totals = data.get("totals", {})
    vendor_name = data.get("vendor_name", "el vendedor")
    days = data.get("days", 30)
    focus_stage = data.get("stage", "")          # e.g. "interaccion_leve"
    vendor_breakdown = data.get("vendors", [])   # list of {vendor_name, count, frio, tibio, caliente}

    def pct(a, b): return round(a / b * 100, 1) if b > 0 else 0

    # ── Stage-focused analysis (used by director stage filter view) ──────────
    if focus_stage:
        STAGE_LABELS = {
            "no_respondido": "No Respondido",
            "interaccion_leve": "Interacción Leve",
            "conversacion_fluida": "Conversación Fluida",
            "potencial_compra": "Potencial Compra",
            "venta_realizada": "Venta Realizada",
        }
        NEXT_STAGE = {
            "no_respondido": "Interacción Leve",
            "interaccion_leve": "Conversación Fluida",
            "conversacion_fluida": "Potencial Compra",
            "potencial_compra": "Venta Realizada",
            "venta_realizada": "(ya es la etapa final)",
        }
        stage_label = STAGE_LABELS.get(focus_stage, focus_stage)
        next_stage = NEXT_STAGE.get(focus_stage, "la siguiente etapa")
        stage_total = int(totals.get(focus_stage, 0))
        frio_k = f"{focus_stage}_frio"
        tibio_k = f"{focus_stage}_tibio"
        caliente_k = f"{focus_stage}_caliente"
        frio = int(totals.get(frio_k, 0))
        tibio = int(totals.get(tibio_k, 0))
        caliente = int(totals.get(caliente_k, 0))
        has_temp = frio + tibio + caliente > 0

        vendor_lines = ""
        if vendor_breakdown:
            vendor_lines = "\nDETALLE POR VENDEDOR en esta etapa:\n"
            for vb in sorted(vendor_breakdown, key=lambda x: x.get("count", 0), reverse=True):
                line = f"  - {vb['vendor_name']}: {vb['count']} leads"
                if vb.get("frio") is not None:
                    line += f" (❄️{vb['frio']} 🌡️{vb.get('tibio',0)} 🔥{vb.get('caliente',0)})"
                vendor_lines += line + "\n"

        temp_line = f"\nDistribución de temperatura: ❄️ Frío:{frio} / 🌡️ Tibio:{tibio} / 🔥 Caliente:{caliente}" if has_temp else ""

        prompt = f"""Sos un experto en estrategia comercial y psicología de ventas aplicada a equipos de venta digital.

El director comercial está analizando la etapa "{stage_label}" del funnel de su equipo de {len(vendor_breakdown) or 10} vendedores.

DATOS DE LA ETAPA "{stage_label.upper()}":
- Total de leads en esta etapa: {stage_total}{temp_line}
{vendor_lines}
CONTEXTO: Esta etapa precede a "{next_stage}". El objetivo es entender por qué tantos leads están acumulados aquí y no avanzan.

---

Generá un análisis estratégico en español argentino con esta estructura exacta:

## 🔍 ¿Por qué están acumulados en {stage_label}?
[2-3 oraciones: diagnóstico psicológico y comercial del patrón. ¿Qué está bloqueando el avance? Basate en comportamiento del comprador digital]

## 🧠 Plan de acción inmediato para el equipo (esta semana)
[4-5 acciones concretas que los vendedores pueden tomar YA para mover estos leads a "{next_stage}". Para cada acción: nombre, por qué funciona, y MENSAJE O GUIÓN EXACTO para usar con el lead]

## 📊 Cómo leer la temperatura en {stage_label}
[Explica qué significa tener muchos Frío/Tibio/Caliente en esta etapa y qué hacer diferente con cada temperatura. Incluí mensajes específicos para Tibio y Caliente]

## 🎯 Vendedor a priorizar y por qué
[Basándote en los datos por vendedor, ¿a quién ayudar primero y qué hacer específicamente con ese vendedor para mejorar sus números en esta etapa?]

## 📈 Meta para los próximos 5 días
[Un número concreto: cuántos leads de {stage_label} deberían pasar a {next_stage} esta semana, y la acción específica del equipo para lograrlo]

Sé MUY específico. Usá ejemplos de mensajes reales. No des consejos genéricos."""

        try:
            genai.configure(api_key=config.GEMINI_API_KEY)
            model = genai.GenerativeModel(config.GEMINI_MODEL)
            response = model.generate_content(prompt)
            strategy_text = response.text.strip()
        except Exception as e:
            logger.error("KPI stage strategy error: %s", e)
            return jsonify({"ok": False, "error": "Error al generar análisis"}), 500

        return jsonify({
            "ok": True,
            "strategy": strategy_text,
            "bottleneck": stage_label,
            "bottleneck_rate": 0,
        })

    # ── Full funnel analysis (single vendor or all-vendor summary) ────────────
    nr = int(totals.get("no_respondido", 0))
    il = int(totals.get("interaccion_leve", 0) or
             sum(int(totals.get(k, 0)) for k in ["interaccion_leve_frio","interaccion_leve_tibio","interaccion_leve_caliente"]))
    cf = int(totals.get("conversacion_fluida", 0) or
             sum(int(totals.get(k, 0)) for k in ["conversacion_fluida_frio","conversacion_fluida_tibio","conversacion_fluida_caliente"]))
    pc = int(totals.get("potencial_compra", 0) or
             sum(int(totals.get(k, 0)) for k in ["potencial_compra_frio","potencial_compra_tibio","potencial_compra_caliente"]))
    vr = int(totals.get("venta_realizada", 0))
    total = nr + il + cf + pc + vr

    responded = il + cf + pc + vr
    conv_respuesta = pct(responded, total)
    conv_escalada = pct(cf + pc + vr, responded) if responded > 0 else 0
    conv_interes = pct(pc + vr, cf + pc + vr) if (cf + pc + vr) > 0 else 0
    conv_cierre = pct(vr, pc + vr) if (pc + vr) > 0 else 0

    rates = [("respuesta (apertura/primer mensaje)", conv_respuesta),
             ("escalada (pasar de charla leve a conversación real)", conv_escalada),
             ("generación de interés/deseo de compra", conv_interes),
             ("cierre (convertir interés en venta)", conv_cierre)]
    bottleneck = min(rates, key=lambda x: x[1])

    il_breakdown = f"Frío:{totals.get('interaccion_leve_frio',0)} / Tibio:{totals.get('interaccion_leve_tibio',0)} / Caliente:{totals.get('interaccion_leve_caliente',0)}"
    cf_breakdown = f"Frío:{totals.get('conversacion_fluida_frio',0)} / Tibio:{totals.get('conversacion_fluida_tibio',0)} / Caliente:{totals.get('conversacion_fluida_caliente',0)}"
    pc_breakdown = f"Frío:{totals.get('potencial_compra_frio',0)} / Tibio:{totals.get('potencial_compra_tibio',0)} / Caliente:{totals.get('potencial_compra_caliente',0)}"

    prompt = f"""Sos un experto en estrategia comercial y psicología de ventas. Analizás el funnel de un vendedor y dás estrategias concretas, accionables y basadas en evidencia.

DATOS DEL VENDEDOR: {vendor_name}
Período analizado: últimos {days} días

FUNNEL DE CONVERSACIÓN:
- No Respondido: {nr}
- Interacción Leve: {il} ({il_breakdown})
- Conversación Fluida: {cf} ({cf_breakdown})
- Potencial Compra: {pc} ({pc_breakdown})
- Venta Realizada: {vr}
- Total leads trabajados: {total}

TASAS DE CONVERSIÓN:
- Tasa de respuesta: {conv_respuesta}%
- Escalada (IL → CF): {conv_escalada}%
- Generación de interés (CF → PC): {conv_interes}%
- Tasa de cierre (PC → Venta): {conv_cierre}%

CUELLO DE BOTELLA PRINCIPAL IDENTIFICADO: {bottleneck[0]} ({bottleneck[1]}%)

---

Basándote en esta información, entregá un análisis estratégico en español argentino con esta estructura:

## 🔍 Diagnóstico del cuello de botella
[2-3 oraciones explicando por qué está pasando esto psicológicamente — basado en comportamiento del comprador]

## 🧠 Estrategias inmediatas (esta semana)
[4-5 estrategias concretas y accionables basadas en neuroventas, psicología de ventas, sesgos cognitivos (scarcity, social proof, reciprocity, anchoring, etc.). Para cada estrategia: nombre, explicación breve, y MENSAJE EXACTO o ACCIÓN ESPECÍFICA para implementarla]

## 📊 Qué observar en los números de "tibio" y "caliente"
[Cómo interpretar la distribución frío/tibio/caliente en cada etapa y qué significa para la estrategia de seguimiento]

## 💡 Idea de seguimiento diferencial
[Una táctica específica para mover los leads "tibios" de {bottleneck[0].split('(')[0].strip()} hacia la siguiente etapa — con mensaje o acción lista para usar]

## 📈 Meta realista para los próximos 7 días
[Un número concreto y una acción específica para mejorar la tasa de {bottleneck[0].split('(')[0].strip()}]

Sé MUY específico. Incluí frases, mensajes y ejemplos reales. No des consejos genéricos."""

    try:
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(config.GEMINI_MODEL)
        response = model.generate_content(prompt)
        strategy_text = response.text.strip()
    except Exception as e:
        logger.error("KPI strategy error: %s", e)
        return jsonify({"ok": False, "error": "Error al generar estrategias"}), 500

    return jsonify({
        "ok": True,
        "strategy": strategy_text,
        "bottleneck": bottleneck[0],
        "bottleneck_rate": bottleneck[1],
        "conv_rates": {
            "respuesta": conv_respuesta,
            "escalada": conv_escalada,
            "interes": conv_interes,
            "cierre": conv_cierre,
        }
    })


@app.route("/lanzamiento/kpi/director")
def lanzamiento_kpi_director():
    redir = _require_vendor()
    if redir: return redir
    from collections import defaultdict

    from_date = request.args.get("from_date", "")
    to_date = request.args.get("to_date", "")
    vendor_filter = request.args.get("vendor_id", "")
    stage_filter = request.args.get("stage", "")
    vid = int(vendor_filter) if vendor_filter else None

    entries = database.kpi_get_all_entries(
        from_date=from_date or None,
        to_date=to_date or None,
        vendor_id=vid
    )
    # All-vendor summaries (unfiltered by vendor for comparison chart)
    vendor_summaries = database.kpi_get_all_vendors_summary(
        from_date=from_date or None,
        to_date=to_date or None
    )
    global_totals = database.kpi_aggregate_entries(entries)
    vendors_list = database.get_kpi_vendors()

    # Daily series: group by date
    daily_by_date: dict = defaultdict(list)
    for e in entries:
        daily_by_date[e["entry_date"]].append(e)
    daily_series = sorted([
        {"date": d, "totals": database.kpi_aggregate_entries(es)}
        for d, es in daily_by_date.items()
    ], key=lambda x: x["date"])

    # Yesterday's breakdown by vendor (vendors load at end of day)
    from datetime import timedelta
    today = datetime.now(TZ).date().isoformat()
    yesterday = (datetime.now(TZ).date() - timedelta(days=1)).isoformat()
    yest_all_entries = database.kpi_get_all_entries(from_date=yesterday, to_date=yesterday)
    today_entries = [e for e in yest_all_entries]  # "today" slot = yesterday data
    today_totals = database.kpi_aggregate_entries(today_entries)
    today_by_vendor = sorted([
        {
            "vendor_id": e["vendor_id"],
            "vendor_name": e.get("vendor_name", ""),
            "photo_path": e.get("photo_path"),
            "totals": database.kpi_aggregate_entries([e])
        }
        for e in today_entries
    ], key=lambda x: x["totals"].get("venta_realizada", 0), reverse=True)

    # Daily activity detail: per-date, per-vendor breakdown
    # Build: {date -> {vendor_id -> entry}}
    daily_vendor_map: dict = defaultdict(dict)
    for e in database.kpi_get_all_entries(from_date=from_date or None, to_date=to_date or None):
        daily_vendor_map[e["entry_date"]][e["vendor_id"]] = e

    kpi_vendor_ids = {v["id"] for v in vendors_list}
    daily_activity = sorted([
        {
            "date": d,
            "loaded": sorted([
                {"vendor_id": vid2, "vendor_name": e.get("vendor_name",""), "photo_path": e.get("photo_path"),
                 "venta_realizada": int(e.get("venta_realizada",0) or 0),
                 "total_leads": int(e.get("no_respondido",0) or 0) + int(e.get("interaccion_leve_frio",0) or 0) + int(e.get("interaccion_leve_tibio",0) or 0) + int(e.get("interaccion_leve_caliente",0) or 0) + int(e.get("conversacion_fluida_frio",0) or 0) + int(e.get("conversacion_fluida_tibio",0) or 0) + int(e.get("conversacion_fluida_caliente",0) or 0) + int(e.get("potencial_compra_frio",0) or 0) + int(e.get("potencial_compra_tibio",0) or 0) + int(e.get("potencial_compra_caliente",0) or 0) + int(e.get("venta_realizada",0) or 0),
                }
                for vid2, e in vendor_map.items()
            ], key=lambda x: x["vendor_name"]),
            "missing": sorted([
                v for v in vendors_list if v["id"] not in vendor_map
            ], key=lambda x: x["name"]),
        }
        for d, vendor_map in daily_vendor_map.items()
    ], key=lambda x: x["date"], reverse=True)

    custom_labels = database.kpi_get_active_labels()
    director_goal = database.get_director_goal()

    # Per-day and grand totals for table subtotal rows
    import json as _json_tbl
    daily_entry_sums: dict = {}
    grand_entry_total = {"nr": 0, "il": 0, "cf": 0, "pc": 0, "vr": 0, "custom": {}}
    for e in entries:
        d = e.get("entry_date", "")
        if d not in daily_entry_sums:
            daily_entry_sums[d] = {"nr": 0, "il": 0, "cf": 0, "pc": 0, "vr": 0, "custom": {}}
        ds = daily_entry_sums[d]
        nr = int(e.get("no_respondido", 0) or 0)
        il = sum(int(e.get(k, 0) or 0) for k in ["interaccion_leve_frio", "interaccion_leve_tibio", "interaccion_leve_caliente"])
        cf = sum(int(e.get(k, 0) or 0) for k in ["conversacion_fluida_frio", "conversacion_fluida_tibio", "conversacion_fluida_caliente"])
        pc = sum(int(e.get(k, 0) or 0) for k in ["potencial_compra_frio", "potencial_compra_tibio", "potencial_compra_caliente"])
        vr = int(e.get("venta_realizada", 0) or 0)
        for key, val in [("nr", nr), ("il", il), ("cf", cf), ("pc", pc), ("vr", vr)]:
            ds[key] += val
            grand_entry_total[key] += val
        try:
            cv = _json_tbl.loads(e.get("custom_values") or "{}")
        except Exception:
            cv = {}
        for lid, val in cv.items():
            v = int(val or 0)
            ds["custom"][str(lid)] = ds["custom"].get(str(lid), 0) + v
            grand_entry_total["custom"][str(lid)] = grand_entry_total["custom"].get(str(lid), 0) + v

    return render_template("lanzamiento_kpi_director.html",
                           entries=entries,
                           vendor_summaries=vendor_summaries,
                           global_totals=global_totals,
                           vendors_list=vendors_list,
                           daily_series=daily_series,
                           today_totals=today_totals,
                           daily_activity=daily_activity,
                           today_by_vendor=today_by_vendor,
                           today=today,
                           yesterday=yesterday,
                           from_date=from_date, to_date=to_date,
                           vendor_filter=vendor_filter,
                           stage_filter=stage_filter,
                           custom_labels=custom_labels,
                           director_goal=director_goal,
                           daily_entry_sums=daily_entry_sums,
                           grand_entry_total=grand_entry_total)


@app.route("/lanzamiento/kpi/director/save-goal", methods=["POST"])
def lanzamiento_kpi_director_save_goal():
    redir = _require_vendor()
    if redir: return jsonify({"ok": False}), 403
    data = request.get_json()
    goal = int(data.get("goal", 0) or 0)
    database.save_director_goal(goal)
    return jsonify({"ok": True, "goal": goal})


@app.route("/lanzamiento/kpi/director/diagnostico", methods=["POST"])
def lanzamiento_kpi_director_diagnostico():
    import google.generativeai as genai
    data = request.get_json()
    totals = data.get("totals", {})
    vendor_summaries = data.get("vendor_summaries", [])
    goal = int(data.get("goal", 0) or 0)

    def pct(a, b): return round(a / b * 100, 1) if b > 0 else 0

    nr  = int(totals.get("no_respondido", 0))
    il  = int(totals.get("interaccion_leve", 0))
    cf  = int(totals.get("conversacion_fluida", 0))
    pc  = int(totals.get("potencial_compra", 0))
    vr  = int(totals.get("venta_realizada", 0))
    total = nr + il + cf + pc + vr
    responded = il + cf + pc + vr
    cf_plus = cf + pc + vr
    pc_plus = pc + vr

    conv_resp = pct(responded, total)
    conv_esc  = pct(cf_plus, responded)
    conv_pc   = pct(pc_plus, cf_plus)
    conv_vr   = pct(vr, pc_plus)

    bottleneck_map = [
        ("Tasa de Respuesta (NR→IL)", conv_resp),
        ("Escalada a Conversación (IL→CF)", conv_esc),
        ("Generación de Interés (CF→PC)", conv_pc),
        ("Cierre (PC→Venta)", conv_vr),
    ]
    bottleneck = min(bottleneck_map, key=lambda x: x[1])

    vendor_lines = "\n".join(
        f"  • {vs.get('vendor_name','?')}: {vs.get('venta_realizada',0)} ventas / "
        f"{vs.get('potencial_compra',0)} PC / {vs.get('conversacion_fluida',0)} CF / "
        f"{vs.get('interaccion_leve',0)} IL / {vs.get('no_respondido',0)} NR"
        for vs in vendor_summaries
    )
    goal_line = f"\nOBJETIVO DEL DIRECTOR: {goal} ventas. Progreso actual: {vr}/{goal} ({pct(vr,goal)}%)" if goal > 0 else ""

    prompt = f"""Sos un consultor experto en dirección comercial y psicología de equipos de ventas.
Tu rol es diagnosticar el estado de un lanzamiento de programa de formación en ventas,
detectar los cuellos de botella reales y proponer estrategias de liderazgo accionables.

Aplicás los principios de:
- John Maxwell (El líder de 360°, Las 21 leyes irrefutables del liderazgo): liderazgo situacional, desarrollo de personas, visión compartida
- Dale Carnegie (Cómo ganar amigos e influir sobre las personas): motivación intrínseca, reconocimiento, comunicación efectiva
- Robert Greene (Las 48 leyes del poder, La ley 50): maestría táctica, lectura del entorno, adaptación estratégica

DATOS DEL LANZAMIENTO (acumulado):
- Total leads trabajados: {total}
- No respondidos: {nr} ({pct(nr,total)}%)
- Interacción Leve: {il}
- Conversación Fluida: {cf}
- Potencial Compra: {pc}
- Ventas concretadas: {vr}
{goal_line}

TASAS DE CONVERSIÓN DEL FUNNEL:
- Respuesta: {conv_resp}%  (NR → IL)
- Escalada: {conv_esc}%    (IL → CF)
- Interés: {conv_pc}%      (CF → PC)
- Cierre: {conv_vr}%       (PC → Venta)

CUELLO DE BOTELLA DETECTADO: {bottleneck[0]} ({bottleneck[1]}%)

BREAKDOWN POR VENDEDOR:
{vendor_lines}

---
Generá un diagnóstico comercial completo con este formato exacto:

## 🔍 Diagnóstico General
(2-3 líneas evaluando el estado real del lanzamiento y la salud del funnel)

## ⚠️ Cuello de Botella Principal
(Explicá por qué {bottleneck[0]} es el mayor freno y qué lo causa en equipos de venta de programas digitales)

## 🎯 Estrategias de Dirección (Maxwell)
(2-3 acciones de liderazgo situacional para que el director desbloquee el equipo)

## 💬 Cultura de Equipo (Carnegie)
(2 acciones concretas para motivar, reconocer y alinear al equipo emocionalmente)

## ♟️ Movimientos Tácticos (Greene)
(2 movimientos estratégicos de corto plazo para reactivar el lanzamiento)

## 📈 Plan de Acción Inmediato
(3 acciones específicas a ejecutar en las próximas 48-72 hs para mejorar los números)

Sé directo, concreto y sin relleno. Cada punto debe ser accionable."""

    try:
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(config.GEMINI_MODEL)
        response = model.generate_content(prompt)
        text = response.text.strip() if response.text else ""
        if not text:
            return jsonify({"ok": False, "error": "La IA no devolvió respuesta. Intentá de nuevo."}), 200
        return jsonify({"ok": True, "analysis": text,
                        "bottleneck": bottleneck[0], "bottleneck_rate": bottleneck[1]})
    except Exception as e:
        logger.error("Director diagnostico error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 200


@app.route("/lanzamiento/director")
def lanzamiento_director():
    redir = _require_vendor()
    if redir: return redir
    vendor_stats = database.lanzamiento_get_vendor_stats()
    return render_template("lanzamiento_director.html", vendor_stats=vendor_stats)


@app.route("/lanzamiento")
def lanzamiento():
    redir = _require_vendor()
    if redir: return redir
    import json as _json
    vendors_list = database.get_vendors()
    submissions = database.lanzamiento_get_recent(limit=200)
    # Parse section_scores for display
    for s in submissions:
        if s.get("section_scores"):
            try:
                s["section_scores_dict"] = _json.loads(s["section_scores"])
            except Exception:
                s["section_scores_dict"] = {}
        else:
            s["section_scores_dict"] = {}
    return render_template("lanzamiento.html", vendors=vendors_list, submissions=submissions)


@app.route("/lanzamiento/upload", methods=["POST"])
def lanzamiento_upload():
    vendor_name = (request.form.get("vendor_name") or "").strip()
    if not vendor_name:
        flash("Seleccioná un vendedor.", "danger")
        return redirect(url_for("lanzamiento"))

    if "file" not in request.files or request.files["file"].filename == "":
        flash("Seleccioná un archivo (video o captura de pantalla).", "danger")
        return redirect(url_for("lanzamiento"))

    file = request.files["file"]
    if not _allowed_lanzamiento(file.filename):
        flash("Formato no soportado. Usá: mp4, mov, jpg, jpeg, png, webp", "danger")
        return redirect(url_for("lanzamiento"))

    # Optional: phase-specific analysis
    analysis_phase = (request.form.get("analysis_phase") or "").strip() or None
    custom_instructions = (request.form.get("custom_instructions") or "").strip() or None

    ext = file.filename.rsplit(".", 1)[1].lower()
    file_id = str(uuid.uuid4())
    saved_name = f"{file_id}.{ext}"
    file_path = os.path.join(LANZAMIENTO_FOLDER, saved_name)
    file_type = "image" if ext in {"jpg", "jpeg", "png", "webp", "gif"} else "video"

    try:
        chunk_size = 8 * 1024 * 1024
        with open(file_path, "wb") as f:
            while True:
                chunk = file.stream.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as exc:
        if os.path.exists(file_path):
            os.unlink(file_path)
        flash(f"Error al guardar el archivo: {exc}", "danger")
        return redirect(url_for("lanzamiento"))

    database.lanzamiento_mark_processing(file_id, file.filename, vendor_name, file_type,
                                         analysis_phase=analysis_phase,
                                         custom_instructions=custom_instructions)
    _lanzamiento_queue.put({
        "file_id": file_id,
        "file_path": file_path,
        "vendor_name": vendor_name,
        "file_name": file.filename,
        "analysis_phase": analysis_phase,
        "custom_instructions": custom_instructions,
    })

    phase_label = {"relacion": "Relación", "descubrimiento": "Descubrimiento",
                   "siembra": "Siembra", "objeciones": "Objeciones"}.get(analysis_phase, "")
    phase_msg = f" (enfocado en {phase_label})" if phase_label else ""
    flash(f"Análisis de {vendor_name}{phase_msg} en proceso. El feedback estará listo en unos minutos.", "success")
    return redirect(url_for("lanzamiento"))


@app.route("/lanzamiento/feedback/<file_id>")
def lanzamiento_feedback_json(file_id: str):
    import json as _json
    submissions = database.lanzamiento_get_recent(limit=500)
    for s in submissions:
        if s["file_id"] == file_id:
            result = {
                "status": s["status"],
                "feedback": s.get("feedback_text", ""),
                "score": s.get("score"),
                "vendor_name": s["vendor_name"],
                "error": s.get("error_message", ""),
            }
            if s.get("section_scores"):
                try:
                    result["section_scores"] = _json.loads(s["section_scores"])
                except Exception:
                    result["section_scores"] = {}
            return jsonify(result)
    return jsonify({"error": "not found"}), 404


@app.route("/lanzamiento/submissions/<file_id>", methods=["DELETE"])
def lanzamiento_delete_submission(file_id: str):
    deleted = database.lanzamiento_delete_submission(file_id)
    if deleted:
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "not found"}), 404


@app.route("/lanzamiento/print/<file_id>")
def lanzamiento_print(file_id: str):
    """Returns a standalone print-ready HTML page for the feedback."""
    submissions = database.lanzamiento_get_recent(limit=500)
    for s in submissions:
        if s["file_id"] == file_id:
            if s["status"] != "done":
                return "El feedback aún no está disponible.", 404
            from flask import render_template
            return render_template(
                "lanzamiento_print.html",
                vendor_name=s["vendor_name"],
                feedback_text=s.get("feedback_text", ""),
                score=s.get("score"),
                submitted_at=s.get("submitted_at", ""),
            )
    return "Feedback no encontrado.", 404


# ── Análisis de Chats ──────────────────────────────────────────────────────────

@app.route("/analisis-chats")
def analisis_chats():
    redir = _require_producer()
    if redir:
        return redir
    import json as _json
    analysis_path = os.path.join(_BASE_DIR, "chat_analysis.json")
    data = {}
    if os.path.exists(analysis_path):
        with open(analysis_path, encoding="utf-8") as f:
            data = _json.load(f)
    return render_template("analisis_chats.html", data=data)


# ── Feedback & status APIs ─────────────────────────────────────────────────────

@app.route("/feedback/<file_id>")
def get_feedback(file_id: str):
    import json as _json
    records = database.get_recent_records(limit=1000)
    for r in records:
        if r["file_id"] == file_id:
            result = {
                "feedback": r.get("feedback_text", ""),
                "vendor_name": r["vendor_name"],
                "score": r.get("score"),
                "processed_at": r.get("processed_at", ""),
            }
            if r.get("section_scores"):
                try:
                    result["section_scores"] = _json.loads(r["section_scores"])
                except Exception:
                    result["section_scores"] = {}
            return jsonify(result)
    return jsonify({"error": "not found"}), 404


@app.route("/roleplay/print/<file_id>")
def roleplay_print(file_id: str):
    """Standalone print-ready HTML page for Roleplay VH feedback."""
    import json as _json
    records = database.get_recent_records(limit=1000)
    for r in records:
        if r["file_id"] == file_id:
            if r["status"] != "done":
                return "El feedback aún no está disponible.", 404
            section_scores_dict = {}
            if r.get("section_scores"):
                try:
                    section_scores_dict = _json.loads(r["section_scores"])
                except Exception:
                    pass
            return render_template(
                "roleplay_print.html",
                vendor_name=r["vendor_name"],
                score=r.get("score"),
                processed_at=r.get("processed_at", ""),
                feedback_text=r.get("feedback_text", ""),
                section_scores_dict=section_scores_dict,
            )
    return "Registro no encontrado.", 404


@app.route("/status")
def status():
    return jsonify({
        "today_count": database.count_today(),
        "pending_count": database.count_pending(),
        "queue_size": _processing_queue.qsize(),
        "missing_configs": config.get_missing_configs(),
    })


# ── App factory ────────────────────────────────────────────────────────────────

def _cleanup_on_startup():
    """Al arrancar: limpia archivos de video huérfanos y resetea registros colgados."""
    # Resetear registros que quedaron en 'processing' de una sesión anterior
    reset_count = database.reset_stuck_processing()
    if reset_count:
        logger.warning("Reset %d stuck 'processing' records to 'error'.", reset_count)

    # Eliminar solo archivos temporales huérfanos (sin registro en DB) del uploads/ raíz
    try:
        known_ids = {r["file_id"] for r in database.get_recent_records(limit=10000)}
        deleted = 0
        for fname in os.listdir(UPLOAD_FOLDER):
            fpath = os.path.join(UPLOAD_FOLDER, fname)
            if not os.path.isfile(fpath):
                continue
            ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
            if ext not in ALLOWED_EXTENSIONS:
                continue
            file_id = fname.rsplit(".", 1)[0]
            if file_id not in known_ids:
                os.unlink(fpath)
                deleted += 1
        if deleted:
            logger.info("Cleaned up %d orphaned video file(s) from uploads/.", deleted)
    except Exception as e:
        logger.warning("Error during upload cleanup: %s", e)

    # Eliminar archivos de roleplay de submissions ya procesadas (done/error) — el feedback está en la DB
    try:
        done_ids = {r["file_id"] for r in database.get_recent_records(limit=10000)
                    if r.get("status") in ("done", "error")}
        freed = 0
        for folder in (UPLOAD_FOLDER, ROLEPLAYS_FOLDER):
            if not os.path.isdir(folder):
                continue
            for fname in os.listdir(folder):
                fpath = os.path.join(folder, fname)
                if not os.path.isfile(fpath):
                    continue
                file_id = fname.rsplit(".", 1)[0]
                if file_id in done_ids:
                    sz = os.path.getsize(fpath)
                    os.unlink(fpath)
                    freed += sz
        if freed:
            logger.info("Freed %.1f MB from processed roleplay files.", freed / 1024 / 1024)
    except Exception as e:
        logger.warning("Error during processed-file cleanup: %s", e)

    # Eliminar archivos de lanzamiento de submissions ya procesadas
    try:
        done_lanz_ids = {r["file_id"] for r in database.lanzamiento_get_recent(limit=5000)
                         if r.get("status") in ("done", "error")}
        freed_lanz = 0
        if os.path.isdir(LANZAMIENTO_FOLDER):
            for fname in os.listdir(LANZAMIENTO_FOLDER):
                fpath = os.path.join(LANZAMIENTO_FOLDER, fname)
                if not os.path.isfile(fpath):
                    continue
                file_id = fname.rsplit(".", 1)[0]
                if file_id in done_lanz_ids:
                    sz = os.path.getsize(fpath)
                    os.unlink(fpath)
                    freed_lanz += sz
        if freed_lanz:
            logger.info("Freed %.1f MB from processed lanzamiento files.", freed_lanz / 1024 / 1024)
    except Exception as e:
        logger.warning("Error during lanzamiento file cleanup: %s", e)


def create_app():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(PHOTO_FOLDER, exist_ok=True)
    os.makedirs(LANZAMIENTO_FOLDER, exist_ok=True)
    database.init_db()
    database.seed_system_leads()
    _cleanup_on_startup()
    worker = threading.Thread(target=_worker, daemon=True, name="VideoWorker")
    worker.start()
    logger.info("Video processing worker started.")
    lanz_worker = threading.Thread(target=_lanzamiento_worker, daemon=True, name="LanzamientoWorker")
    lanz_worker.start()
    logger.info("Lanzamiento worker started.")
    return app


if __name__ == "__main__":
    application = create_app()
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
