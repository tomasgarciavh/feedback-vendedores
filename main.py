import json
import logging
import os
import queue
import threading
import uuid

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
ALLOWED_EXTENSIONS = {"mp4", "mov", "avi", "mkv", "webm", "m4v", "wmv", "flv", "3gp"}
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
        try:
            import gemini_analyzer
            raw_feedback = gemini_analyzer.analyze_lanzamiento(
                file_path=file_path,
                vendor_name=vendor_name,
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


@app.after_request
def add_no_cache(response):
    # Skip no-cache for images so the browser can display them
    if response.content_type and response.content_type.startswith("image/"):
        return response
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
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


@app.route("/upload_drive", methods=["POST"])
def upload_drive():
    import gdown
    vendor_name = (request.form.get("vendor_name") or "").strip()
    drive_url = (request.form.get("drive_url") or "").strip()

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
        # gdown handles confirmation pages, large files, and retries automatically
        output = gdown.download(
            id=file_id_drive,
            output=file_path,
            quiet=True,
            fuzzy=True,
        )
        if not output or not os.path.exists(file_path):
            raise RuntimeError("La descarga falló. Verificá que el archivo esté compartido con 'Cualquiera con el link'.")

        # Try to detect real extension from downloaded file
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
        date_label = (r["processed_at"] or "")[:10]
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
    data = database.get_analytics_data()
    return render_template("analytics.html", **data)


@app.route("/formacion")
def formacion_vh():
    return render_template("formacion_vh.html")


# ── Roleplay Chat ──────────────────────────────────────────────────────────────

def _vendor_session():
    """Returns the logged-in vendor dict from Flask session, or None."""
    vid = flask_session.get("chat_vendor_id")
    if not vid:
        return None
    return database.get_vendor_by_id(vid)


def _require_producer():
    """Returns redirect to producer login if not authenticated, else None."""
    if not flask_session.get("producer_auth"):
        return redirect(url_for("productor_login", next=request.path))
    return None


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


@app.route("/chat")
def chat():
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("chat_login"))
    system_leads = database.get_system_leads()
    my_leads = database.get_vendor_leads(vendor["id"])
    sessions = database.get_vendor_sessions(vendor["id"])
    return render_template("chat.html", vendor=vendor,
                           system_leads=system_leads, my_leads=my_leads,
                           sessions=sessions)


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
    return redirect(url_for("chat_login"))


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

    system_prompt = f"""Estás haciendo un roleplay de práctica de ventas para un vendedor de Vendedores Humanos.

Interpretás el personaje: {lead['name']}
Descripción: {lead['description']}
Personalidad y comportamiento: {lead['personality']}
Objeciones típicas: {lead['objections']}

REGLAS:
- Respondé SOLO como {lead['name']}, nunca rompas el personaje.
- Tus respuestas son cortas: 1 a 3 oraciones máximo, como mensajes de WhatsApp.
- Hablá en español argentino informal.
- No digas que sos una IA ni que esto es un roleplay.
- Si el vendedor te pregunta algo relevante sobre tu situación, respondé con detalles verosímiles coherentes con tu perfil.
- Si el vendedor comete errores graves (presión, falta de escucha, promesas vacías), reaccioná enfriándote o poniendo más resistencia.
- Si el vendedor hace un buen trabajo (escucha activa, empatía, preguntas buenas), comenzá a abrirte un poco más.

Historial de la conversación:
{history_text}

Respondé ahora como {lead['name']} (solo tu próximo mensaje, sin incluir tu nombre):"""

    try:
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(config.GEMINI_MODEL)
        response = model.generate_content(system_prompt)
        lead_reply = response.text.strip()
    except Exception as e:
        logger.error("Gemini chat error: %s", e)
        return jsonify({"ok": False, "error": "Error al generar respuesta"}), 500

    messages.append({"role": "lead", "text": lead_reply})
    database.update_session_messages(session_id, _json.dumps(messages))

    return jsonify({"ok": True, "reply": lead_reply, "messages": messages})


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

    feedback_prompt = f"""Sos un coach experto en ventas de la metodología Vendedores Humanos (VH).
Acabás de observar este roleplay de práctica: el VENDEDOR practicó una conversación por chat con el lead "{lead['name']}" ({lead['description']}).

CONVERSACIÓN COMPLETA:
{conversation_text}

---

{config.FEEDBACK_CRITERIA}

---

Analizá la conversación completa y generá un feedback detallado en español argentino con esta estructura EXACTA:

## 🎯 Puntuación General
[Un número del 1 al 10 y una oración de síntesis]

## ✅ Lo que hiciste muy bien
[3-5 puntos concretos con ejemplos de la conversación]

## ⚠️ Áreas a mejorar
[3-5 puntos concretos con qué dijo y qué debería haber dicho]

## 💡 Técnica clave para trabajar
[La técnica VH más importante que necesita practicar, con ejemplo concreto de cómo aplicarla]

## 📊 Puntajes por sección
diagnostico_desapego: [0-10]
descubrimiento_acuerdos: [0-10]
empatia_escucha: [0-10]
ingenieria_preguntas: [0-10]
gestion_creencias: [0-10]
storytelling: [0-10]
pitch_personalizado: [0-10]
mentalidad: [0-10]

## 🔁 Cómo repetir este roleplay mejor
[2-3 recomendaciones específicas para cuando lo vuelva a hacer]
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

    database.close_session(session_id, feedback_text, score, _json.dumps(section_scores))

    return jsonify({"ok": True, "feedback": feedback_text, "score": score,
                    "section_scores": section_scores})


@app.route("/chat/session/<int:session_id>")
def chat_view_session(session_id: int):
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("chat_login"))
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
        },
        {
            "difficulty": "medio", "emoji": "🤔", "name": "Sebastián",
            "short": "Educado pero reservado, hay que ganarse su confianza",
            "context": "Sebastián, 38 años, vendedor de autos. Entró al taller gratis por curiosidad. Es educado pero reservado — responde poco. No quiere que le vendan nada. Hay que ganarse su confianza antes de avanzar.",
        },
        {
            "difficulty": "difícil", "emoji": "😶", "name": "Valeria",
            "short": "Casi no interactúa, responde con monosílabos",
            "context": "Valeria, 44 años, empleada pública. Se inscribió al taller pero casi no interactuó. Responde con monosílabos ('sí', 'ok', 'puede ser'). Hay que generar conexión desde cero con mucha paciencia.",
        },
    ],
    "descubrimiento": [
        {
            "difficulty": "fácil", "emoji": "💬", "name": "Paula",
            "short": "Habla mucho y comparte sus dolores fácilmente",
            "context": "Paula, 29 años, emprendedora que vende productos de limpieza naturales. Quiere crecer pero no sabe cómo. Habla mucho de sus problemas: ingresos estancados, cansancio, siente que trabaja sola. Ideal para practicar descubrimiento activo.",
        },
        {
            "difficulty": "medio", "emoji": "🔒", "name": "Marcos",
            "short": "No abre el juego solo, hay que preguntar bien",
            "context": "Marcos, 41 años, contador con consultora propia pero ingresos irregulares. Es reservado con sus miedos. Responde a preguntas concretas pero no abre el juego solo. Hay que preguntar bien para que profundice.",
        },
        {
            "difficulty": "difícil", "emoji": "🛡️", "name": "Romina",
            "short": "Se pone a la defensiva si preguntás mucho",
            "context": "Romina, 36 años, vendedora freelance de seguros. Interpreta las preguntas como un interrogatorio. Si preguntás mucho dice '¿para qué necesitás saber eso?' o 'estoy bien como estoy'. Hay que avanzar muy despacio y con mucha empatía.",
        },
    ],
    "siembra": [
        {
            "difficulty": "fácil", "emoji": "🌱", "name": "Florencia",
            "short": "Receptiva, escucha bien y se emociona con las historias",
            "context": "Florencia, 27 años, quiere salir de su trabajo en relación de dependencia. Ya confía en el vendedor. Escucha, hace preguntas, se emociona con historias. Es el momento de sembrar bien: conectar su situación con casos de éxito y el programa.",
        },
        {
            "difficulty": "medio", "emoji": "😐", "name": "Gustavo",
            "short": "Le entran algunos hooks pero dice 'mi caso es diferente'",
            "context": "Gustavo, 45 años, dueño de una ferretería. Quiere crecer pero no conecta con historias ajenas fácilmente. Dice 'mi caso es diferente'. Hay que sembrar con ejemplos muy específicos parecidos a su situación.",
        },
        {
            "difficulty": "difícil", "emoji": "🧱", "name": "Alejandro",
            "short": "No cree en los cursos, mucha resistencia",
            "context": "Alejandro, 50 años, gerente de ventas con 20 años de experiencia. Cuando mencionás el programa dice 'eso es para gente que no sabe'. No cree en la formación. Hay que sembrar con prueba social muy fuerte sin mencionar el programa directamente.",
        },
    ],
    "objeciones": [
        {
            "difficulty": "fácil", "emoji": "💰", "name": "Natalia",
            "short": "Una sola objeción: el precio",
            "context": "Natalia, 33 años, diseñadora gráfica independiente. Quiere entrar al programa, ya casi convencida. Su única objeción es el precio: 'es mucho'. No tiene objeciones de tiempo ni credibilidad. Practicá el manejo del precio con elegancia y sin dar descuentos.",
        },
        {
            "difficulty": "medio", "emoji": "🤯", "name": "Fernando",
            "short": "Lo tiene que pensar + consultar con su mujer",
            "context": "Fernando, 37 años, comerciante con local propio. Le interesa el programa pero tiene dos objeciones: 'lo tengo que pensar' y 'lo tengo que hablar con mi mujer'. No es que no quiera — necesita validación externa y más tiempo. Practicá el desapego y cierres suaves.",
        },
        {
            "difficulty": "difícil", "emoji": "🔥", "name": "Nicolás",
            "short": "Múltiples objeciones en cadena, la más difícil",
            "context": "Nicolás, 42 años, ex-emprendedor que tuvo un negocio y le fue mal. Múltiples objeciones en cadena: 'es caro', 'no tengo tiempo', 'ya probé cosas así y no funcionaron', 'no sé si esto es para mí'. Cuando resolvés una, aparece la siguiente. Practicá persistencia con desapego.",
        },
    ],
}


@app.route("/chat/lanzamiento")
def chat_lanzamiento():
    vendor = _vendor_session()
    if not vendor:
        return redirect(url_for("chat_login"))
    sessions = database.lanzamiento_coach_get_vendor_sessions(vendor["id"])
    return render_template("chat_lanzamiento.html", vendor=vendor,
                           sessions=sessions, phases=LANZAMIENTO_PHASES,
                           preset_leads=LANZAMIENTO_PRESET_LEADS)


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

        system_prompt = f"""Estás haciendo un roleplay de práctica de ventas para {vendor['name']}, vendedor del lanzamiento digital de Valentín Hernández (VH).

Interpretás el personaje: {lead_name}
Perfil del lead: {sess['lead_context'] or 'Lead genérico que llegó al lanzamiento de VH. Tiene interés pero también dudas.'}

FASE DE PRÁCTICA: {phase_info['label']} ({phase_info['days']})
Objetivo del vendedor en esta fase: {phase_info['goal']}
Cómo debés comportarte en esta fase: {phase_info['lead_behavior']}

REGLAS ESTRICTAS:
- Respondé SOLO como {lead_name}. Nunca rompas el personaje.
- Mensajes cortos: 1 a 3 oraciones máximo. Tono de WhatsApp, argentino informal.
- No digas que sos una IA.
- Si el vendedor usa E.P.P. bien (escucha, participa, profundiza) → abrís un poco más.
- Si el vendedor presiona, vende antes de tiempo, o ignora lo que dijiste → te cerrás, respondés más seco.
- Si el vendedor hace una siembra genuina y conectada con algo que dijiste → mostrás interés real.
- Si el vendedor intenta recomendar el programa antes de haber construido relación y descubrimiento → ponés resistencia ("no sé, no conozco mucho", "voy a pensar").

Historial:
{history_text}

Respondé solo como {lead_name} (tu próximo mensaje, sin poner tu nombre):"""

        try:
            genai.configure(api_key=config.GEMINI_API_KEY)
            model = genai.GenerativeModel(config.GEMINI_MODEL)
            response = model.generate_content(system_prompt)
            lead_reply = response.text.strip()
        except Exception as e:
            logger.error("Gemini lanzamiento roleplay error: %s", e)
            return jsonify({"ok": False, "error": "Error al generar respuesta"}), 500
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass

        messages.append({"role": "lead", "text": lead_reply})
        database.lanzamiento_coach_update_messages(session_id, _json.dumps(messages))
        return jsonify({"ok": True, "reply": lead_reply, "role": "lead"})

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

        coach_prompt = f"""Sos el mejor asesor comercial y coach de ventas del equipo de Valentín Hernández (VH). Tenés dominio absoluto de la metodología: relación profunda, descubrimiento, siembra, storytelling, manejo de objeciones y cierre con desapego. Tu misión es ayudar a {vendor['name']} a cerrar más ventas durante el lanzamiento de 21 días.

━━━ CONTEXTO ━━━

Vendedor: {vendor['name']}
Lead: {lead_name}
Perfil del lead: {sess['lead_context'] or 'Sin contexto registrado aún.'}
Fase del lanzamiento: {phase_info['label']} ({phase_info['days']}) — {phase_info['goal']}{file_identification}

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
[2-3 oraciones: ¿Dónde está emocionalmente este lead? ¿Qué reveló (consciente o inconscientemente) en lo que el vendedor describió o en la imagen? ¿Qué está buscando realmente?]

🎯 **ESTRATEGIA PARA ESTE LEAD**
[La estrategia específica para las próximas 24-48 horas con este lead particular. No genérica — basada en lo que sabemos de él. Incluí qué etapa priorizar, por qué, y qué resultado buscar.]

---

**OPCIÓN A** — 🤝 Conexión y vínculo
> [Mensaje completo, cálido, argentino informal. Listo para copiar. Aplica E.P.P.: escucha + apertura personal + pregunta profunda. Sin emojis forzados.]

**OPCIÓN B** — 🔍 Descubrimiento profundo
> [Mensaje que apunta a uno de los 7 puntos clave: objetivo / dolor / miedo / deseo / situación / problema / costo de oportunidad. Pregunta que toca algo real de su vida. Listo para copiar.]

**OPCIÓN C** — 🌱 Siembra / Elevar nivel de conciencia
> [Mini historia real o reflexión que conecta con algo que el lead dijo. Activa curiosidad, identificación o imaginación. Rompe una creencia limitante de forma sutil. Si no hay suficiente base aún, hacé descubrimiento desde otro ángulo. Listo para copiar.]

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
        database.lanzamiento_coach_close(session_id, feedback_text, score)
        return jsonify({"ok": True, "feedback": feedback_text, "score": score})
    else:
        database.lanzamiento_coach_close(session_id)
        return jsonify({"ok": True, "feedback": "", "score": None})


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
    )


# ── Lanzamiento feedback ──────────────────────────────────────────────────────

@app.route("/lanzamiento")
def lanzamiento():
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
        flash(f"Formato no soportado. Usá: mp4, mov, jpg, jpeg, png, webp", "danger")
        return redirect(url_for("lanzamiento"))

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

    database.lanzamiento_mark_processing(file_id, file.filename, vendor_name, file_type)
    _lanzamiento_queue.put({
        "file_id": file_id,
        "file_path": file_path,
        "vendor_name": vendor_name,
        "file_name": file.filename,
    })

    flash(f"Archivo de {vendor_name} en análisis. El feedback estará listo en unos minutos.", "success")
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
    records = database.get_recent_records(limit=1000)
    for r in records:
        if r["file_id"] == file_id:
            return jsonify({"feedback": r.get("feedback_text", ""), "vendor_name": r["vendor_name"]})
    return jsonify({"error": "not found"}), 404


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
    application.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
