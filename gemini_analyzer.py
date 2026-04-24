import logging
import os
import subprocess
import tempfile
import time

from google import genai

import config

logger = logging.getLogger(__name__)


def _transcode_to_h264(input_path: str) -> str:
    """
    Re-encode video to H.264/AAC mp4 using the bundled ffmpeg from imageio-ffmpeg.
    Returns path to the transcoded file (caller must delete it).
    Raises RuntimeError if transcoding fails.
    """
    try:
        import imageio_ffmpeg
        ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        raise RuntimeError("imageio-ffmpeg no está instalado. Ejecutá: pip install imageio-ffmpeg")

    fd, out_path = tempfile.mkstemp(suffix="_h264.mp4")
    os.close(fd)

    cmd = [
        ffmpeg_bin,
        "-y",               # overwrite output
        "-i", input_path,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        "-max_muxing_queue_size", "1024",
        out_path,
    ]
    logger.info("Transcoding to H.264: %s → %s", input_path, out_path)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        if os.path.exists(out_path):
            os.unlink(out_path)
        raise RuntimeError(
            f"ffmpeg transcoding failed (code {result.returncode}): {result.stderr[-300:]}"
        )
    logger.info("Transcoding complete: %s", out_path)
    return out_path


def _needs_transcode(file_path: str) -> bool:
    """Return True if the file is likely to fail Gemini's codec check."""
    ext = os.path.splitext(file_path)[1].lower().lstrip(".")
    # Always safe formats for Gemini (H.264 mp4 / webm VP8): no transcode needed
    # For everything else transcode to be safe
    try:
        import imageio_ffmpeg
        ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
        probe = subprocess.run(
            [ffmpeg_bin, "-i", file_path],
            capture_output=True, text=True, timeout=30,
        )
        output = probe.stderr
        # If already H.264 in an mp4/mov container, skip transcode
        if ("h264" in output.lower() or "avc" in output.lower()):
            if ext in ("mp4", "m4v", "mov"):
                logger.info("File is H.264/mp4 — skipping transcode")
                return False
    except Exception as e:
        logger.warning("Could not probe video: %s", e)
    return True


def analyze_video(file_path: str, vendor_name: str, criteria: str) -> str:
    """
    Upload a video to Gemini Files API and generate personalized sales feedback
    in a single API call.
    Returns the feedback as a formatted string.
    """
    if not config.GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    client = genai.Client(api_key=config.GEMINI_API_KEY)

    # Transcode to H.264/mp4 if needed so Gemini accepts any input format/codec
    transcoded_path = None
    upload_path = file_path
    try:
        if _needs_transcode(file_path):
            transcoded_path = _transcode_to_h264(file_path)
            upload_path = transcoded_path
    except Exception as exc:
        logger.warning("Transcode failed, uploading original: %s", exc)
        upload_path = file_path

    # Upload file
    logger.info("Uploading video to Gemini Files API: %s", upload_path)
    try:
        from google.genai import types as genai_types
        uploaded_file = client.files.upload(
            file=upload_path,
            config=genai_types.UploadFileConfig(mime_type="video/mp4"),
        )
    except Exception as exc:
        raise RuntimeError(f"Error uploading file to Gemini: {exc}") from exc
    finally:
        if transcoded_path and os.path.exists(transcoded_path):
            try:
                os.unlink(transcoded_path)
            except Exception:
                pass

    # Wait for Gemini to finish processing the video
    wait_seconds = 0
    while uploaded_file.state.name == "PROCESSING":
        if wait_seconds > 600:
            raise RuntimeError("Gemini file processing timed out after 10 minutes.")
        logger.info("Waiting for Gemini to process video... (%ds)", wait_seconds)
        time.sleep(10)
        wait_seconds += 10
        uploaded_file = client.files.get(name=uploaded_file.name)

    if uploaded_file.state.name == "FAILED":
        raise RuntimeError(
            "Gemini failed to process the video file. "
            "Verificá que el formato sea soportado (mp4, mov, avi, mkv, webm)."
        )

    logger.info("Video ready. Generating feedback for %s...", vendor_name)

    language = config.FEEDBACK_LANGUAGE
    prompt = f"""Sos un coach experto en la metodología de Ventas Humanas (VH), inspirada en los principios de Dale Carnegie ("Cómo ganar amigos e influir sobre las personas"), Jürgen Klaric ("Véndele a la mente, no a la gente") y Robert Greene ("Naturaleza Humana").

Tu misión es dar feedback honesto, humano y accionable a vendedores que están aprendiendo esta metodología. El potencial cliente nunca debe sentirse presionado: la venta es una consecuencia natural de una conversación genuina.

La estructura de la venta VH tiene 4 etapas (usá siempre este vocabulario exacto):
1. DESCUBRIMIENTO — la etapa más importante. Incluye: Rapport, Claridad, Tomador de Decisión, Situación Actual, Situación Deseada, GAP, los 5 Acuerdos, Desafíos, Dolores, Costo de Oportunidad, Preferencia, Urgencia, Compromiso y Venta Inversa.
2. DIAGNÓSTICO — qué solución necesita el lead y cuál es el camino personalizado.
3. PERMISO — transición al pitch, siempre pidiendo autorización.
4. PITCH — presentación de pilares conectados a los dolores específicos del lead, con certeza y seguridad.

SISTEMA DE FEEDBACK — SCI + PRÓXIMO PASO:
Cada punto de fortaleza o mejora debe seguir este formato:
"Cuando [situación concreta], [conducta específica del vendedor], [impacto que eso tuvo en el lead o en la venta]. La próxima vez podés [próximo paso accionable]."

Reglas:
- Lenguaje natural, cálido y directo. Nada de jerga corporativa.
- Citá momentos concretos del video siempre que sea posible.
- No te limités a un número fijo de puntos: marcá TODOS los que sean relevantes.
- Usá siempre los términos VH: Descubrimiento, Diagnóstico, Permiso, Pitch.

## CRITERIOS DE EVALUACIÓN
{criteria}

---

Analizá el video de esta llamada realizada por **{vendor_name}** y estructurá tu respuesta exactamente así:

---

### Feedback para {vendor_name}

**Puntuación general:** [X/10] — [una frase que resuma el desempeño overall de forma honesta y humana]

---

### 1. DESCUBRIMIENTO ⭐
*(Esta es la etapa más importante de la venta. Analizala en profundidad.)*

#### Fortalezas en el Descubrimiento
[SCI + Próximo Paso para cada punto. Todos los que correspondan, sin límite.]

#### Puntos de mejora en el Descubrimiento
[SCI + Próximo Paso para cada punto. Todos los que correspondan, sin límite.]

#### Acuerdos VH — ¿Se cumplieron?
- **Acuerdo 1** — El lead quiere mejores resultados: [✓ Sí / ✗ No — breve observación]
- **Acuerdo 2** — Cree que es posible lograrlo: [✓ Sí / ✗ No — breve observación]
- **Acuerdo 3** — Hay un problema específico que lo frena: [✓ Sí / ✗ No — breve observación]
- **Acuerdo 4** — Ese problema le está costando dinero: [✓ Sí / ✗ No — breve observación]
- **Acuerdo 5** — No puede resolverlo solo: [✓ Sí / ✗ No — breve observación]

---

### 2. DIAGNÓSTICO
#### Fortalezas
[SCI + Próximo Paso. Todos los que correspondan.]

#### Puntos de mejora
[SCI + Próximo Paso. Todos los que correspondan.]

---

### 3. PERMISO
#### Fortalezas
[SCI + Próximo Paso. Todos los que correspondan.]

#### Puntos de mejora
[SCI + Próximo Paso. Todos los que correspondan.]

---

### 4. PITCH
**Nivel de certeza y seguridad en el Pitch:** [Alto / Medio / Bajo] — [observación concreta sobre cómo se escuchó]

#### Fortalezas
[SCI + Próximo Paso. Todos los que correspondan.]

#### Puntos de mejora
[SCI + Próximo Paso. Todos los que correspondan.]

---

### 5. MANEJO DE OBJECIONES
*(Solo si hubo objeciones. Si no las hubo, indicarlo.)*

**Nivel de certeza y seguridad al manejar objeciones:** [Alto / Medio / Bajo] — [observación concreta]

#### Fortalezas
[SCI + Próximo Paso.]

#### Puntos de mejora
[SCI + Próximo Paso.]

---

### 6. ESCUCHA ACTIVA, COMUNICACIÓN Y PSICOLOGÍA DE VENTAS
[Evaluá aquí: escucha activa real vs. seguir un guion, parafraseo con las palabras exactas del lead, profundizar respuestas abstractas, creatividad y metáforas, igualación de tono y ritmo, desapego, manejo del ego del cliente, silencio después del precio, terminar intervenciones con una pregunta, y cualquier otro punto de comunicación relevante. Usá SCI + Próximo Paso.]

---

### Mensaje final
[2-3 oraciones de cierre, humanas y personalizadas para {vendor_name}. Reconocé el esfuerzo, señalá el mayor logro de esta llamada y dejalo motivado para la próxima.]

---

### 7. ANÁLISIS FODA PERSONAL

#### 🟢 Fortalezas (lo que ya domina)
[Listá 2-4 habilidades o comportamientos concretos que {vendor_name} ya ejecuta bien según lo visto en esta llamada y las anteriores si las hay. Sé específico, no genérico.]

#### 🔴 Debilidades (lo que necesita trabajar con urgencia)
[Listá 2-4 áreas concretas donde {vendor_name} pierde ventas o rompe el rapport. Identificá el patrón recurrente más importante.]

#### 🟡 Oportunidades (lo que puede aprovechar)
[1-2 habilidades que {vendor_name} tiene en potencia y con práctica pueden convertirse en fortalezas. Sé alentador pero honesto.]

#### ⚫ Amenazas (hábitos o creencias que lo frenan)
[1-2 comportamientos o creencias que, si no se corrigen, van a seguir limitando su desempeño. Pueden ser miedos, hábitos de comunicación, etc.]

---

### 8. PLAN DE ACCIÓN PERSONALIZADO

#### Esta semana — Practicá esto AHORA
[1-2 acciones concretas y simples que {vendor_name} puede implementar en su próxima llamada. Deben ser pequeñas victorias rápidas.]

#### Este mes — Construí estos hábitos
[2-3 hábitos o ejercicios específicos para desarrollar en las próximas semanas, ordenados por prioridad. Incluí cómo practicarlos (role-play, grabarse, etc.).]

#### Clases VH recomendadas para repasar
Basándote en las debilidades detectadas, recomendá exactamente qué repasar del material de formación VH. Usá este mapa completo:

**PILAR 1 — ESTRUCTURA VENTAS HUMANAS**
- Falla en **Rapport / Primera impresión / Sobre-rapport**: → Pilar 1 Clase 2 — Paso 1 RAPPORT (duración, errores, energía del cliente)
- Falla en **Claridad / Sacar presión de venta**: → Pilar 1 Clase 2 — Paso 2 CLARIDAD (comunicar el propósito, ir al grano)
- Falla en **Tomador de decisión / objeciones con pareja o socio**: → Pilar 1 Clase 2 — Paso 3 TOMADOR DE DECISIÓN
- Falla en **Descubrimiento / Situación Actual / re-preguntas**: → Pilar 1 Clase 2 — Paso 4 SITUACIÓN ACTUAL (postura de "admisор", no quedarse en lo superficial)
- Falla en **Situación Deseada / GAP / anclaje a la meta**: → Pilar 1 Clase 2 — Paso 5 SITUACIÓN DESEADA (generar distancia económica y emocional)
- Falla en **Desafíos / profundidad de dolores / problemas indirectos**: → Pilar 1 Clase 2 — Paso 6 DESAFÍOS DEL CLIENTE
- Falla en **Anclar dolores a emociones / sin costo emocional**: → Pilar 1 Clase 2 — Paso 7 DOLORES DEL CLIENTE
- Falla en **Preferencia / venta inversa**: → Pilar 1 Clase 2 — Paso 8 PREFERENCIA
- Falla en **Costo de oportunidad / no cuantifica el problema**: → Pilar 1 Clase 2 — Paso 9 COSTO DE OPORTUNIDAD
- Falla en **Urgencia / no genera urgencia real**: → Pilar 1 Clase 2 — Paso 10 URGENCIA
- Falla en **Compromiso / escala del 1 al 10**: → Pilar 1 Clase 2 — Paso 11 COMPROMISO
- Falla en **Permiso / transición al pitch**: → Pilar 1 Clase 2 — Paso 12 PERMISO
- Falla en **Pitch / no conecta pilares a dolores / palabras prohibidas**: → Pilar 1 Clase 2 — Paso 13 PITCH (3 pasos, checks por pilar, precio sin justificar)
- Falla en **Los 5 Acuerdos VH**: → Pilar 1 Clase 1 (Estructura Macro) — Acuerdos 1 a 5
- Falla en **Fundamentos de objeciones / pierde la calma / confronta**: → Pilar 1 Clase 3 FUNDAMENTOS EN OBJECIONES (13 principios)
- Falla en **Manejo de objeciones específicas** ("lo tengo que pensar", "es muy caro", "no tengo dinero", etc.): → Pilar 1 Clase 4 MANEJO DE OBJECIONES — la objeción específica detectada

**PILAR 2 — NEUROVENTAS Y VENTAS PSICOLÓGICAS**
- Falla en **activar cerebro reptil / emocional / racional**: → Pilar 2 Clase 1 NEUROVENTAS
- Falla en **Storytelling / metáforas / no usa historias**: → Pilar 2 Clase 2 (Idiomas del Cerebro) + Clase 3 (Cómo contar historias)
- Falla en **habla del producto en vez del cliente / habla demasiado de sí mismo**: → Pilar 2 Clase 4 — 6 ERRORES A EVITAR
- Falla en **adaptación al género del cliente**: → Pilar 2 Clase 5 VENDERLE A HOMBRES VS MUJERES
- Falla en **sesgos / certeza / costo hundido / autoridad**: → Pilar 2 Clase 6 SESGOS PSICOLÓGICOS

**PILAR 3 — MENTALIDAD A PRUEBA DE EMOCIONES**
- Falla en **mentalidad / gestión emocional / ansiedad / miedo al rechazo**: → Pilar 3 Clase 1 CÓMO GESTIONAS TUS EMOCIONES
- Falla en **mentalidad de escasez / enfocado en la comisión / no en el cliente**: → Pilar 3 Clase 2 PATRONES DE ALTO RENDIMIENTO (patrón 5)
- Falla en **toma los "no" de forma personal / se frustra con objeciones**: → Pilar 3 Clase 3 GESTIONAR EL RECHAZO
- Falla en **se compara con otros vendedores / baja autoestima**: → Pilar 3 Clase 4 DEJAR DE COMPARARME
- Falla en **rigidez / falta de creatividad en la conversación**: → Pilar 3 Clase 5 DESARROLLAR CREATIVIDAD
- Falla en **falta de certeza / inseguridad en la voz**: → Pilar 3 Clase 6 ALTER EGO + Clase 7 SISTEMA VAKOG

**PILAR 4 — SEGUIMIENTOS**
- Falla en **no hace seguimiento o lo hace de forma agresiva**: → Pilar 4 — los 4 tipos de seguimiento (Valor, Venta, Cierre, Cruzado)

Formato de respuesta para esta sección:
📚 **[Nombre de la Clase]** — [Por qué específicamente {vendor_name} necesita repasarla, conectado a lo visto en la llamada]

---

### SCORES
Incluí exactamente este bloque JSON al final, con los puntajes del 1 al 10 para cada habilidad VH:

- **diagnostico_desapego**: ¿Se contuvo de pitchear antes de tiempo? ¿El cliente pidió la solución porque el diagnóstico fue perfecto?
- **descubrimiento_acuerdos**: ¿Cumplió los 5 acuerdos VH? ¿Hizo preguntas que llegaron al dolor real?
- **empatia_escucha**: ¿Respondió en base a lo que el cliente dijo? ¿El cliente se sintió comprendido?
- **ingenieria_preguntas**: ¿Perforó hasta el dolor real con re-preguntas? ¿El cliente mencionó el costo de oportunidad?
- **gestion_creencias**: ¿Iluminó creencias limitantes sin confrontar? ¿El cliente se sintió empoderado?
- **storytelling**: ¿Usó metáforas e historias con puente claro (mentor/vehículo)?
- **pitch_personalizado**: ¿Unió los pilares al dolor exacto del cliente con sus propias palabras?
- **mentalidad**: ¿Mantuvo neutralidad y energía ante la resistencia o el "no"?

```json_scores
{{
  "score_general": 0,
  "diagnostico_desapego": 0,
  "descubrimiento_acuerdos": 0,
  "empatia_escucha": 0,
  "ingenieria_preguntas": 0,
  "gestion_creencias": 0,
  "storytelling": 0,
  "pitch_personalizado": 0,
  "mentalidad": 0
}}
```

Respondé en {language}."""

    try:
        response = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=[uploaded_file, prompt],
        )
    except Exception as exc:
        raise RuntimeError(f"Gemini content generation failed: {exc}") from exc
    finally:
        try:
            client.files.delete(name=uploaded_file.name)
            logger.info("Gemini file deleted: %s", uploaded_file.name)
        except Exception as e:
            logger.warning("Could not delete Gemini file: %s", e)

    # Extract text robustly — response.text is None when safety filters block output
    feedback = None
    try:
        feedback = response.text
    except Exception:
        pass

    if not feedback or not feedback.strip():
        # Log the finish reason so we know why it's empty
        finish_reason = "unknown"
        try:
            finish_reason = str(response.candidates[0].finish_reason)
        except Exception:
            pass
        logger.error("Gemini returned empty response. finish_reason=%s", finish_reason)

        # If blocked by safety filters, try again without the video context in the prompt
        # by extracting any partial text from candidates
        partial = ""
        try:
            for candidate in response.candidates:
                for part in candidate.content.parts:
                    if hasattr(part, "text") and part.text:
                        partial += part.text
        except Exception:
            pass

        if partial.strip():
            logger.info("Recovered partial text from candidates (%d chars)", len(partial))
            feedback = partial
        else:
            raise RuntimeError(
                f"Gemini no pudo generar el feedback (finish_reason={finish_reason}). "
                "Esto puede deberse a los filtros de seguridad del modelo o a un video demasiado largo. "
                "Intentá con un video más corto o verificá el contenido del archivo."
            )

    logger.info("Feedback generated. Length: %d chars", len(feedback))
    return feedback


_EXTERNAL_RESOURCES = """
━━━ BIBLIOTECA DE RECURSOS EXTERNOS ━━━

Cuando detectes una debilidad específica, recomendá uno o más recursos externos además del material VH. Usá SOLO los recursos de esta lista y siempre incluí el link exacto.

📚 LIBROS — con enlace de búsqueda Amazon:

COMUNICACIÓN Y PERSUASIÓN:
- "Influence: The Psychology of Persuasion" — Robert Cialdini → https://www.amazon.com/s?k=influence+the+psychology+of+persuasion+cialdini
  *Cuándo recomendarlo:* falla en sesgos, no sabe crear urgencia, no usa prueba social, no genera autoridad.

- "Pre-Suasion" — Robert Cialdini → https://www.amazon.com/s?k=pre-suasion+cialdini
  *Cuándo recomendarlo:* los mensajes llegan en el momento equivocado, no prepara el terreno antes de hablar del programa.

- "How to Win Friends and Influence People" — Dale Carnegie → https://www.amazon.com/s?k=how+to+win+friends+and+influence+people+carnegie
  *Cuándo recomendarlo:* falla en relación, rapport, conexión genuina, escucha activa.

- "Véndele a la Mente, No a la Gente" — Jürgen Klaric → https://www.amazon.com/s?k=vendele+a+la+mente+no+a+la+gente+klaric
  *Cuándo recomendarlo:* argumenta lógicamente en lugar de conectar emocionalmente, habla de características en lugar de sensaciones.

NEGOCIACIÓN:
- "Never Split the Difference" — Chris Voss → https://www.amazon.com/s?k=never+split+the+difference+voss
  *Cuándo recomendarlo:* falla en manejo de objeciones, presiona en lugar de escuchar, no valida las emociones del lead, no usa etiquetado emocional.

- "Getting to Yes" — Fisher & Ury → https://www.amazon.com/s?k=getting+to+yes+fisher+ury
  *Cuándo recomendarlo:* entra en conflicto con el lead, no separa la persona del problema, negocia posiciones en lugar de intereses.

- "Pitch Anything" — Oren Klaff → https://www.amazon.com/s?k=pitch+anything+oren+klaff
  *Cuándo recomendarlo:* el pitch no tiene estructura, no genera tensión de estatus, el lead no siente escasez real.

VENTAS:
- "SPIN Selling" — Neil Rackham → https://www.amazon.com/s?k=spin+selling+neil+rackham
  *Cuándo recomendarlo:* no usa preguntas de implicación ni de necesidad/payoff, descubrimiento superficial.

- "The Challenger Sale" — Dixon & Adamson → https://www.amazon.com/s?k=the+challenger+sale+dixon+adamson
  *Cuándo recomendarlo:* no enseña nada nuevo al lead, no reencuadra la situación, sigue el guión en lugar de enseñar.

- "To Sell Is Human" — Daniel Pink → https://www.amazon.com/s?k=to+sell+is+human+daniel+pink
  *Cuándo recomendarlo:* mentalidad negativa sobre las ventas, cree que "vender es manipular", falta de propósito.

- "Fanatical Prospecting" — Jeb Blount → https://www.amazon.com/s?k=fanatical+prospecting+jeb+blount
  *Cuándo recomendarlo:* no hace seguimiento consistente, abandona leads fácilmente, no tiene un sistema de contacto.

ORATORIA Y COMUNICACIÓN:
- "Talk Like TED" — Carmine Gallo → https://www.amazon.com/s?k=talk+like+ted+carmine+gallo
  *Cuándo recomendarlo:* storytelling débil, no conecta emocionalmente con historias, los mensajes son aburridos o planos.

- "Simply Speaking" — Peggy Noonan → https://www.amazon.com/s?k=simply+speaking+peggy+noonan
  *Cuándo recomendarlo:* mensajes demasiado complejos, no es claro, usa jerga, el lead no entiende el valor.

- "The Art of Explanation" — Lee LeFever → https://www.amazon.com/s?k=the+art+of+explanation+lefever
  *Cuándo recomendarlo:* no puede explicar el programa de forma simple, se pierde en detalles técnicos.

MENTALIDAD Y ALTO RENDIMIENTO:
- "Mindset: The New Psychology of Success" — Carol Dweck → https://www.amazon.com/s?k=mindset+the+new+psychology+of+success+carol+dweck
  *Cuándo recomendarlo:* miedo al fracaso, se rinde ante el primer "no", mentalidad fija sobre sus habilidades de ventas.

- "The Way of the Wolf" — Jordan Belfort → https://www.amazon.com/s?k=the+way+of+the+wolf+jordan+belfort
  *Cuándo recomendarlo:* falta de certeza en la voz, no transmite seguridad, no cierra con convicción.

- "Atomic Habits" — James Clear → https://www.amazon.com/s?k=atomic+habits+james+clear
  *Cuándo recomendarlo:* no tiene consistencia en el seguimiento, olvida leads, no construye rutinas de venta.

- "Man's Search for Meaning" — Viktor Frankl → https://www.amazon.com/s?k=man+search+for+meaning+viktor+frankl
  *Cuándo recomendarlo:* crisis de propósito, vende por comisión y no por ayudar, se afecta emocionalmente con el rechazo.

PSICOLOGÍA:
- "Thinking, Fast and Slow" — Daniel Kahneman → https://www.amazon.com/s?k=thinking+fast+and+slow+kahneman
  *Cuándo recomendarlo:* no entiende cómo toman decisiones los leads, no sabe cuándo hablar al sistema 1 (emocional) vs sistema 2 (racional).

- "Predictably Irrational" — Dan Ariely → https://www.amazon.com/s?k=predictably+irrational+dan+ariely
  *Cuándo recomendarlo:* no usa anclaje de precio, no entiende el efecto de la gratuidad, no aprovecha la comparación relativa.

🌐 RECURSOS WEB — artículos, blogs y cursos con link directo:

VENTAS:
- Blog de Sales Hacker (mejores prácticas de ventas modernas) → https://www.saleshacker.com/blog/
  *Cuándo recomendarlo:* necesita actualizar técnicas de prospección, seguimiento y cierre.

- HubSpot Sales Blog (guías de ventas consultivas) → https://blog.hubspot.com/sales
  *Cuándo recomendarlo:* necesita mejorar la estructura de conversaciones de venta y uso de CRM mental.

- Artículo "The SPIN Selling Methodology Explained" — HubSpot → https://blog.hubspot.com/sales/spin-selling
  *Cuándo recomendarlo:* no domina preguntas de descubrimiento profundo.

COMUNICACIÓN Y STORYTELLING:
- TED Masterclass de Storytelling → https://www.masterclass.com/articles/how-to-tell-a-story
  *Cuándo recomendarlo:* siembra débil, no conecta con historias, mensajes planos.

- Artículo "How to Use Storytelling in Sales" — HubSpot → https://blog.hubspot.com/sales/storytelling-in-sales
  *Cuándo recomendarlo:* no usa mini historias o las usa de forma forzada.

MENTALIDAD:
- Blog de James Clear (hábitos y mentalidad) → https://jamesclear.com/articles
  *Cuándo recomendarlo:* problemas de consistencia, gestión del tiempo, hábitos de seguimiento.

- Artículo "Growth Mindset vs Fixed Mindset" → https://fs.blog/carol-dweck-mindset/
  *Cuándo recomendarlo:* se bloquea ante el rechazo, cree que "no es bueno para las ventas".

NEGOCIACIÓN:
- Blog del Programa de Negociación de Harvard (PON) → https://www.pon.harvard.edu/blog/
  *Cuándo recomendarlo:* falla en negociación de precio, no sabe cómo crear acuerdos mutuamente beneficiosos.

- Artículo "How to Use the Tactical Empathy" — Black Swan Group → https://www.blackswanltd.com/the-edge
  *Cuándo recomendarlo:* no valida emociones del lead, pelea contra las objeciones en lugar de escucharlas.

━━━ CÓMO INCLUIR ESTOS RECURSOS ━━━

En la sección "PLAN DE ACCIÓN" y "RECOMENDACIONES DE ESTUDIO", para cada debilidad detectada incluí:
1. El recurso VH específico (clase del programa)
2. UNO O DOS recursos externos de esta lista (libro + artículo web cuando sea posible)
3. Por qué específicamente este recurso para esta persona y esta debilidad
4. El link exacto, siempre clickeable

Formato de presentación:
📚 **[Título del libro/recurso]** — [Autor/Fuente]
🔗 [link exacto]
💡 *Por qué para {vendor_name}:* [conexión directa con lo observado en el chat]
"""


def analyze_lanzamiento(file_path: str, vendor_name: str,
                        analysis_phase: str = None,
                        custom_instructions: str = None) -> str:
    """
    Analyzes a WhatsApp conversation (video screen-recording OR image screenshot)
    and returns structured SCI + Próximo Paso feedback for the 21-day launch methodology.
    If analysis_phase is set, the analysis focuses on that specific phase.
    If custom_instructions is set, those instructions guide the analysis.
    """
    if not config.GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not configured.")

    client = genai.Client(api_key=config.GEMINI_API_KEY)

    ext = os.path.splitext(file_path)[1].lower().lstrip(".")
    image_exts = {"jpg", "jpeg", "png", "webp", "gif"}
    video_exts = {"mp4", "mov", "avi", "mkv", "webm", "m4v", "wmv", "flv", "3gp"}

    if ext in image_exts:
        mime_map = {
            "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
            "webp": "image/webp", "gif": "image/gif",
        }
        mime_type = mime_map.get(ext, "image/jpeg")
    else:
        mime_map = {
            "mp4": "video/mp4", "mov": "video/quicktime", "avi": "video/x-msvideo",
            "mkv": "video/x-matroska", "webm": "video/webm", "m4v": "video/mp4",
            "wmv": "video/x-ms-wmv", "flv": "video/x-flv", "3gp": "video/3gpp",
        }
        mime_type = mime_map.get(ext, "video/mp4")

    # For video files, transcode to H.264 first to ensure Gemini compatibility
    transcoded_path = None
    upload_path = file_path
    if ext not in image_exts:
        try:
            if _needs_transcode(file_path):
                transcoded_path = _transcode_to_h264(file_path)
                upload_path = transcoded_path
                mime_type = "video/mp4"
        except Exception as exc:
            logger.warning("Transcode failed, uploading original: %s", exc)
            upload_path = file_path

    logger.info("Uploading lanzamiento file to Gemini: %s (%s)", upload_path, mime_type)
    try:
        from google.genai import types as genai_types
        uploaded_file = client.files.upload(
            file=upload_path,
            config=genai_types.UploadFileConfig(mime_type=mime_type),
        )
    except Exception as exc:
        raise RuntimeError(f"Error uploading file to Gemini: {exc}") from exc
    finally:
        if transcoded_path and os.path.exists(transcoded_path):
            try:
                os.unlink(transcoded_path)
            except Exception:
                pass

    # Wait for processing (videos only — images are instant)
    wait_seconds = 0
    while uploaded_file.state.name == "PROCESSING":
        if wait_seconds > 600:
            raise RuntimeError("Gemini file processing timed out after 10 minutes.")
        logger.info("Waiting for Gemini to process file... (%ds)", wait_seconds)
        time.sleep(10)
        wait_seconds += 10
        uploaded_file = client.files.get(name=uploaded_file.name)

    if uploaded_file.state.name == "FAILED":
        raise RuntimeError("Gemini failed to process the file. Verificá el formato (mp4, mov, jpg, png, webp).")

    # ── Phase-specific focus block ──────────────────────────────────────────
    PHASE_LABELS = {
        "relacion":       ("RELACIÓN", "Días 1–5", "Crear vínculo genuino usando E.P.P. Activar reciprocidad emocional, identificación y espejo. NO mencionar clases ni programa todavía."),
        "descubrimiento": ("DESCUBRIMIENTO", "Días 5–10", "Mapear dolores profundos, miedos, deseos, situación actual, problemas y costo de oportunidad. Preguntas de re-pregunta. Los 7 puntos clave."),
        "siembra":        ("SIEMBRA & STORYTELLING", "Días 10–16", "Mini historias reales que generan curiosidad, identificación e imaginación. Romper creencias limitantes. Invitar a la Clase 2 conectándola con el dolor específico del lead."),
        "objeciones":     ("OBJECIONES & RECOMENDACIÓN", "Días 16–21", "Manejar objeciones con preguntas y acuerdos, nunca argumentando. Recomendación personalizada que une el programa con los dolores exactos del lead."),
    }

    phase_focus_block = ""
    if analysis_phase and analysis_phase in PHASE_LABELS:
        ph_label, ph_days, ph_goal = PHASE_LABELS[analysis_phase]
        phase_focus_block = f"""
---

## ⚠️ ANÁLISIS ENFOCADO — SOLO ESTA ETAPA

Esta sesión de análisis está enfocada **únicamente en la etapa de {ph_label}** ({ph_days}).

**Objetivo de esta etapa:** {ph_goal}

INSTRUCCIONES ESPECIALES:
- Ignorá las etapas que NO sean {ph_label}. No las evalúes ni las comentes.
- Todo el análisis, el FODA, el plan de acción y las recomendaciones de estudio deben girar 100% alrededor de cómo el vendedor ejecutó (o no) la etapa de {ph_label}.
- En la sección de scores, ponés 0 en todas las etapas que no sean {ph_label}.
- El plan de acción debe ser 100% específico para mejorar en {ph_label}.
- Las recomendaciones de recursos (VH + externos) deben ser las más relevantes para {ph_label}.
"""

    custom_block = ""
    if custom_instructions and custom_instructions.strip():
        custom_block = f"""
---

## 📋 INSTRUCCIONES ESPECIALES DEL INSTRUCTOR

{custom_instructions.strip()}

Seguí estas instrucciones al pie de la letra. Tienen prioridad sobre cualquier estructura estándar si hay conflicto.
"""

    prompt = f"""Sos un coach experto en ventas conversacionales por WhatsApp para lanzamientos digitales de 21 días, especializado en psicología de ventas y neuroventas.
Tu misión: dar feedback honesto, humano y profundamente accionable a **{vendor_name}**, un asesor comercial que está aprendiendo el proceso de venta conversacional por WhatsApp.
{phase_focus_block}{custom_block}

---

## CONTEXTO DEL LANZAMIENTO

El proceso dura **21 días**, con 3 clases dominicales y podcasts diarios que nutren al lead. Los asesores acompañan a los leads de forma personalizada por WhatsApp. La filosofía es de **ventas humanas**: nunca presionar, la venta es consecuencia natural de una relación genuina y un diagnóstico profundo.

**OBJETIVO CONCRETO DEL LANZAMIENTO (MUY IMPORTANTE):**
- **Semana 1 y 2**: El objetivo de los asesores es ÚNICAMENTE invitar al lead a la Clase 2 del taller gratuito. NO vender el programa todavía. Generar conexión, elevar el nivel de conciencia, sembrar curiosidad.
- **Semana 3**: Invitar a la Clase 3 (también gratuita, la final), y recién ahí hacer la recomendación personalizada del programa si el lead está listo.
- En NINGÚN momento de las primeras dos semanas se vende el programa pago. Solo se siembra, se nutre, y se conecta emocionalmente.
- La clave está en **conectar cada clase gratuita con el dolor específico de ese lead particular**, porque esa personalización es lo que más convierte.
- El lead siempre tiene que sentir que él está eligiendo estar ahí, que nadie lo está convenciendo de nada.

**Las 4 etapas del proceso:**
1. **RELACIÓN** — Crear vínculo genuino antes de mencionar cualquier clase o programa.
2. **DESCUBRIMIENTO** — Conocer los dolores profundos, miedos y deseos del lead, más de lo que él mismo puede articular.
3. **SIEMBRA / MINI STORYTELLING** — Sembrar curiosidad, identificación e imaginación usando historias reales. Romper creencias limitantes sin confrontar. LUEGO invitar a la clase conectándola con su dolor específico.
4. **RECOMENDACIÓN PERSONALIZADA** — Solo en semana 3. Conectar el programa con los dolores exactos del lead usando sus propias palabras.

**Cómo se invita a la clase correctamente:**
Mal: "Hay una clase este domingo, no te la pierdas."
Bien: "Con todo lo que me contaste de [su situación específica], creo que la clase de este domingo te va a hablar directamente a vos. En esa clase Valentín habla específicamente de [cómo eso conecta con el tema de la clase]. ¿Querés que te cuente de qué trata?"

---

## METODOLOGÍA DETALLADA

### ETAPA 1 — RELACIÓN (Profundidad psicológica)

El objetivo NO es solo ser simpático. Es activar mecanismos psicológicos que generan vínculo genuino y apertura emocional.

**Fórmula E.P.P. (Escucho – Participo – Profundizo):**
- *Escucho + valoro*: "Te re entiendo...", "Qué importante eso que decís.", "Me imagino lo que debe ser..."
- *Participo*: El vendedor se abre PRIMERO con 1 frase personal y vulnerable, sin robar el protagonismo. Esto activa reciprocidad emocional: cuando alguien se abre, el otro también se abre.
- *Profundizo*: Termino con pregunta abierta sobre lo que le interesa al lead. "¿A vos también te pasó?", "¿Cómo lo estás manejando hoy?", "¿Sentís que eso te frena?"

**Sesgos y principios psicológicos clave para la relación:**
- **Reciprocidad emocional**: Abrirse primero genera que el otro se abra. El vendedor comparte algo genuino y personal antes de preguntar.
- **Igualdad/Identificación**: "Yo también pasé por eso" o "Hablar con vos es como hablar con mi yo de hace 3 años" → el lead siente que el vendedor lo entiende DE VERDAD, no solo dice que lo entiende.
- **Efecto espejo**: Usar las mismas palabras y expresiones que usa el lead al responder. Si dice "agotado", el vendedor responde con "agotado", no con "cansado".
- **Pertenencia/Tribu**: Hacer sentir al lead que hay otros como él, que no está solo en esto. "Mucha gente que está donde vos estás..."
- **Consistencia**: Recordar y repetir lo que el lead dijo en mensajes anteriores. "Hace unos días me contaste que..." → genera impacto emocional ("wow, se acordó de mí").
- **Curiosidad genuina**: Hacer preguntas sobre los temas que importan AL LEAD (su familia, su trabajo, sus sueños, su historia), no preguntas genéricas de ventas.
- **Validación emocional**: Antes de cualquier pregunta de descubrimiento, validar cómo se siente el lead sobre lo que acaba de compartir.

**Señales de que la relación está bien construida:** el lead cuenta cosas personales sin que se las pregunten, usa expresiones informales, hace preguntas por iniciativa propia, dice "con vos me siento cómodo para preguntar".

---

### ETAPA 2 — DESCUBRIMIENTO (Mapa completo del lead)

El descubrimiento profundo es la base de todo. Sin él, la siembra y la recomendación son genéricas y no convierten.

**El vendedor debe encontrar (sin orden fijo, de forma natural en la conversación):**

*Sobre sus problemas:*
- **Dolores principales**: El problema más grande que tiene hoy. El que le quita el sueño.
- **Dolores y problemas indirectos**: Las consecuencias colaterales que ese dolor principal le genera. (ej: si el dolor es "no tengo ingresos estables", el indirecto puede ser "discuto con mi pareja", "no puedo irme de vacaciones", "me siento un fracasado").
- **Miedos inconscientes**: Lo que teme que pase si no cambia nada. Muchas veces el lead no lo dice explícitamente pero lo deja entrever. El vendedor debe nombrarlo suavemente.
- **Desafíos concretos**: Las barreras prácticas que enfrenta hoy para lograr lo que quiere.
- **Costo de oportunidad**: ¿Qué le está costando (en tiempo, dinero, energía, relaciones) seguir como está?

*Sobre sus deseos:*
- **Objetivos personales**: ¿Qué quiere lograr en los próximos 6-12 meses?
- **Deseos internos profundos**: ¿Qué hay debajo del objetivo declarado? (ej: dice "quiero ganar más plata" pero en realidad quiere "sentirme valorado y que mi familia esté orgullosa de mí").
- **Deseos personales**: Qué cosas concretas quiere poder hacer/tener/vivir cuando resuelva su situación.
- **Motivaciones de compra**: ¿Qué lo haría decidir sumarse? ¿Qué necesita ver/escuchar para confiar?

*Para conectar con el programa:*
- **Puntos de conexión con VH**: Momentos donde lo que el lead dijo se puede conectar directamente con un pilar del programa. El vendedor debe identificarlos y guardarlos para la siembra/recomendación.
- **Creencias limitantes detectadas**: Frases del lead que revelan una creencia que lo frena ("eso es para otros", "yo no tengo el perfil", "es muy tarde para mí").

**Técnicas de profundización:**
- Re-preguntas: "¿Y eso cómo te afecta en el día a día?"
- Clarificación: "¿A qué te referís cuando decís X? Porque quiero entender cómo lo vivís vos."
- Silencio/espera: después de una pregunta profunda, dejar que el lead termine de escribir sin interrumpir.
- Inversión: "¿Y si esto sigue igual en 1 año, cómo te imaginas?"

---

### ETAPA 3 — SIEMBRA Y MINI STORYTELLING (La etapa más crítica)

El objetivo de la siembra NO es presentar el programa. Es generar 4 cosas en la mente del lead:
1. **Curiosidad**: "¿Esto realmente funciona? ¿Querés contarme más?"
2. **Identificación**: "Eso le pasó a alguien exactamente igual a mí."
3. **Imaginación**: "Si le pasó a él/ella... ¿podría pasarme a mí también?"
4. **Ruptura de creencias limitantes**: La historia demuestra que la creencia del lead ("no tengo el perfil", "es muy tarde", "no sé de tecnología") es falsa con un ejemplo concreto.

**El lead tiene que sentir que está eligiendo. Nunca que lo están convenciendo.**

**Fórmula de siembra:**
Datos del contexto del lead (algo que contó) + Conflicto idéntico o similar al del lead + Cómo alguien con ese conflicto lo resolvió con el programa + Resultado concreto que tuvo (específico y creíble, no mágico).

Ejemplo: *"Con todo lo que me contaste me hiciste acordar a Nati, que entró hace 8 meses. Ella también trabajaba para el estado y sentía que estaba dando horas de su vida a algo que no la movía para nada. Tampoco tenía experiencia en ventas, le daba vergüenza hablar con desconocidos. Y lo que logró fue... [resultado concreto]. Lo que más me sorprendió fue que ella decía exactamente lo mismo que vos recién."*

**Sesgos que activa una siembra bien hecha:**
- **Prueba social específica**: No "muchos lo lograron", sino "Nati, de Mendoza, 34 años, que estaba exactamente donde vos estás".
- **Efecto halo**: La historia de éxito de alguien parecido proyecta ese éxito sobre el lead inconscientemente.
- **Contraste**: Mostrar el antes y el después del personaje de la historia activa el deseo.
- **Esperanza realista**: El resultado debe ser creíble, no exagerado. El lead tiene que pensar "eso lo puedo lograr yo también", no "eso es imposible".

**Errores graves en la siembra:**
- Contar la historia sin conectarla con algo que el lead dijo antes (no hay identificación).
- Hacer el resultado sonar mágico o exagerado (genera desconfianza).
- Terminar la historia sin pregunta abierta (se pierde la devolución del lead).
- Presentar el programa DENTRO de la siembra (rompe el desapego).

---

### ETAPA 4 — RECOMENDACIÓN PERSONALIZADA

Solo después de las etapas anteriores. La recomendación conecta los pilares del programa con los dolores y deseos exactos del lead, usando sus propias palabras.

**Manejo de objeciones:** siempre preguntando y acordando, nunca argumentando.
- "Pensemos lo juntos..."
- "Vos sabías que el fundador de Starbucks recibió 200 mil rechazos antes de llegar a donde llegó?"
- Para métodos de pago: "¿A vos te quedaría cómodo hacerlo con Binance?" → Si sí → pedir email y felicitar.

---

## IDENTIFICACIÓN DE LOS PARTICIPANTES (MUY IMPORTANTE — leer antes de analizar)

El archivo que recibís es una captura de pantalla o grabación de pantalla del WhatsApp del **vendedor {vendor_name}**.

**REGLA PRINCIPAL — identificá por el COLOR DE FONDO del mensaje (es lo más confiable):**
- **Mensajes con fondo VERDE** → son de **{vendor_name} (EL VENDEDOR)**. Siempre.
- **Mensajes con fondo GRIS o BLANCO** → son del **LEAD (el prospecto)**. Siempre.

**Regla secundaria — posición en pantalla (usala solo si el color no es claro):**
- Los mensajes del **VENDEDOR ({vendor_name})** aparecen en el **lado DERECHO**.
- Los mensajes del **LEAD** aparecen en el **lado IZQUIERDO**.

**NUNCA inviertas los roles.** El color de fondo es la señal definitiva: VERDE = vendedor, GRIS = lead. Es un error gravísimo confundir los mensajes del vendedor con los del lead o viceversa — todo el feedback quedaría al revés.

---

## SISTEMA DE FEEDBACK — SCI + PRÓXIMO PASO

Cada punto debe seguir este formato exacto:
*"Cuando [situación concreta del chat con cita textual si es posible], [conducta específica de {vendor_name}], [impacto concreto que eso tuvo en el lead o en la conversación]. La próxima vez podés [próximo paso accionable y específico]."*

Reglas:
- Citá mensajes textuales del chat siempre que sea posible, indicando si es el vendedor o el lead quien lo dijo.
- Lenguaje cálido, directo, argentino. Nada de jerga corporativa.
- Marcá TODOS los puntos relevantes, sin límite de cantidad.
- Si el archivo es una imagen, analizá los mensajes visibles. Si es video, toda la conversación.
- Sé específico sobre QUÉ hizo bien o mal y CÓMO se ve eso en la conversación.

---

Analizá lo que hizo **{vendor_name}** y respondé exactamente con esta estructura:

---

### Feedback para {vendor_name}

**Puntuación general:** [X/10] — [frase honesta y humana que resuma el desempeño overall]

---

### 1. CONSTRUCCIÓN DE RELACIÓN ❤️

**¿Se usó E.P.P.?** [Sí / Parcialmente / No — observación concreta]
**¿El vendedor se abrió primero (reciprocidad emocional)?** [Sí / No — cómo se vio]
**¿Se usaron los sesgos de identificación, espejo, consistencia?** [Qué se usó y qué faltó]

#### Fortalezas
[SCI + Próximo Paso. Todos los que correspondan.]

#### Puntos de mejora
[SCI + Próximo Paso. Todos los que correspondan.]

#### Oportunidades perdidas de conexión
[Momentos específicos donde el lead dio una apertura y el vendedor no la aprovechó. Para cada uno: (1) qué dijo el lead, (2) qué hizo {vendor_name}, (3) cómo debería haberlo respondido. Mostrá el mensaje alternativo ideal.]

---

### 2. DESCUBRIMIENTO 🔍

**Mapa del lead — Los 7 puntos clave de ventas:**

| Punto | ¿Cubierto? | Detalle |
|-------|-----------|---------|
| Objetivos (¿qué quiere lograr?) | ✓/✗ | [lo que dijo o "No explorado"] |
| Dolores principales (¿qué le duele hoy?) | ✓/✗ | [hallazgos o "No identificado"] |
| Miedos (¿qué teme que pase si no cambia?) | ✓/✗ | [hallazgos o "No explorado"] |
| Deseos profundos (¿qué hay debajo del objetivo?) | ✓/✗ | [hallazgos o "No explorado"] |
| Situación actual (¿dónde está hoy?) | ✓/✗ | [hallazgos o "No explorado"] |
| Problemas/obstáculos concretos | ✓/✗ | [hallazgos o "No explorado"] |
| Costo de oportunidad (¿qué le cuesta seguir igual?) | ✓/✗ | [hallazgos o "No abordado"] |

**Creencias limitantes detectadas:** [frases literales del lead que revelan una creencia que lo frena, o "Ninguna detectada"]
**Puntos de conexión con la Clase 2/3 o el programa:** [qué de lo que contó el lead se puede conectar con la clase o el programa]

#### Fortalezas
[SCI + Próximo Paso. Con citas textuales del chat cuando sea posible.]

#### Puntos de mejora
[SCI + Próximo Paso. Cada punto con un EJEMPLO CONCRETO de cómo hubiera podido hacerse mejor. Ej: "Cuando el lead dijo 'no tengo tiempo', podrías haber preguntado: '¿Y cómo te sentís con eso? ¿Sentís que eso te frena para avanzar en lo que querés?'" — muestra siempre el mensaje alternativo que {vendor_name} podría haber enviado.]

#### Preguntas que FALTARON hacer
[Listá las preguntas concretas que {vendor_name} debería haber hecho y no hizo. Para cada una: mostrá el MOMENTO exacto en el chat donde debería haberla hecho y el texto exacto de la pregunta.]

---

### 3. SIEMBRA Y MINI STORYTELLING 🌱

**¿Hubo siembra?** [Sí / No / Parcial]
**¿Se conectó con algo que el lead dijo?** [Sí / No — cómo]
**¿Activó curiosidad, identificación, imaginación y ruptura de creencia?** [Análisis de cada uno]
**¿El lead sintió desapego (que él elige)?** [Sí / No — evidencia en el chat]

#### Fortalezas
[SCI + Próximo Paso.]

#### Puntos de mejora
[SCI + Próximo Paso.]

#### Cómo hubiera sido una siembra ideal para este lead
[Escribí un ejemplo concreto de cómo debería haber sido la siembra para ESTE lead específico, usando lo que se descubrió de él en la conversación. Sé específico y creativo.]

---

### 4. INVITACIÓN A LA CLASE / RECOMENDACIÓN PERSONALIZADA 🎯

*(Si estamos en semana 1-2: evaluar cómo se invitó a la Clase 2 gratuita. Si es semana 3: evaluar la recomendación del programa.)*

**¿Se conectó la clase/programa con el dolor específico del lead?** [Sí / No / Parcialmente — evidencia concreta]
**¿El lead sintió que era para él en particular, o fue una invitación genérica?** [Análisis]

#### Fortalezas
[SCI + Próximo Paso. Con ejemplo del chat.]

#### Puntos de mejora
[SCI + Próximo Paso. Para cada mejora, mostrá CÓMO debería haber sido la invitación/recomendación para este lead específico, usando sus propias palabras y su situación.]

---

### 5. MANEJO DE OBJECIONES 🛡️
*(Solo si hubo objeciones. Si no, indicarlo.)*

#### Fortalezas
[SCI + Próximo Paso.]

#### Puntos de mejora
[SCI + Próximo Paso. Para cada objeción mal manejada, mostrá el MENSAJE ALTERNATIVO ideal — la respuesta que {vendor_name} podría haber enviado en cambio.]

---

### 6. PSICOLOGÍA DE LA CONVERSACIÓN 🧠
[Evaluá: uso de sesgos psicológicos, efecto espejo con las palabras del lead, preguntas abiertas al cerrar cada mensaje, audios de voz, recordar frases del lead en días posteriores, tono y calidez, uso del nombre, velocidad de respuesta y energía general. Para cada punto, citá el ejemplo del chat y mostrá la versión mejorada si aplica. Usá SCI + Próximo Paso.]

---

### Mensaje final
[2-3 oraciones de cierre humanas y personalizadas para {vendor_name}. Reconocé el esfuerzo con algo concreto que viste en la conversación, señalá el mayor logro y dejalo motivado.]

---

### 7. ANÁLISIS FODA PERSONAL

#### 🟢 Fortalezas
[2-4 habilidades concretas que {vendor_name} ya ejecuta bien. Específico, no genérico.]

#### 🔴 Debilidades prioritarias
[2-4 áreas donde pierde conexión o confianza. Identificá el patrón más dañino.]

#### 🟡 Oportunidades
[1-2 habilidades en potencia que con práctica pueden ser fortalezas.]

#### ⚫ Amenazas
[1-2 hábitos o creencias que, si no se corrigen, van a seguir limitando los resultados.]

---

### 8. PLAN DE ACCIÓN

#### Esta semana — Una sola cosa
[La acción más impactante que {vendor_name} puede implementar en el próximo chat. Concreta, simple, una sola cosa.]

#### Este mes — Construí estos hábitos
[2-3 hábitos específicos con instrucciones de cómo practicarlos (role-play, grabarse, etc.).]

#### La pregunta que lo cambiaría todo
[La pregunta que {vendor_name} podría haber hecho en algún momento de ESTE chat que hubiera cambiado completamente la dirección de la conversación. Sé específico con el momento y la pregunta.]

---

### 9. RECOMENDACIONES DE ESTUDIO Y FORMACIÓN 📖

#### Clases VH recomendadas
[Listá las clases VH específicas más relevantes para las debilidades detectadas. Conectalas directamente con lo observado en el chat.]

#### Recursos externos recomendados
{_EXTERNAL_RESOURCES}

Basándote en las debilidades específicas de {vendor_name} en ESTE chat, elegí de la biblioteca de arriba los 3-5 recursos más relevantes. Para cada uno: el título, el link, y por qué específicamente le sirve a {vendor_name} en base a lo que viste.

---

### SCORES
Incluí exactamente este bloque JSON al final:

```json_scores
{{
  "score_general": 0,
  "relacion": 0,
  "descubrimiento": 0,
  "siembra": 0,
  "recomendacion": 0,
  "objeciones": 0,
  "epp_formula": 0,
  "comunicacion": 0,
  "mentalidad": 0
}}
```

Respondé en español (Argentina)."""

    try:
        response = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=[uploaded_file, prompt],
        )
    except Exception as exc:
        raise RuntimeError(f"Gemini content generation failed: {exc}") from exc
    finally:
        try:
            client.files.delete(name=uploaded_file.name)
            logger.info("Gemini file deleted: %s", uploaded_file.name)
        except Exception as e:
            logger.warning("Could not delete Gemini file: %s", e)

    feedback = None
    try:
        feedback = response.text
    except Exception:
        pass

    if not feedback or not feedback.strip():
        finish_reason = "unknown"
        try:
            finish_reason = str(response.candidates[0].finish_reason)
        except Exception:
            pass
        partial = ""
        try:
            for candidate in response.candidates:
                for part in candidate.content.parts:
                    if hasattr(part, "text") and part.text:
                        partial += part.text
        except Exception:
            pass
        if partial.strip():
            feedback = partial
        else:
            raise RuntimeError(
                f"Gemini no pudo generar el feedback (finish_reason={finish_reason}). "
                "Intentá con un archivo más claro o verificá el contenido."
            )

    logger.info("Lanzamiento feedback generated. Length: %d chars", len(feedback))
    return feedback
