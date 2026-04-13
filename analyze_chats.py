"""
Análisis VH — queries ultra-pequeñas, una por sección, para maximizar output quality
"""
import os, glob, sys, json, re
sys.path.insert(0, '/Users/tomas/Feedback Alumnos VH (Claude)/feedback-vendedores')
import config, google.generativeai as genai
genai.configure(api_key=config.GEMINI_API_KEY)
model = genai.GenerativeModel(config.GEMINI_MODEL)

SKIP = ['archivo adjunto','<multimedia omitido>','.opus','.mp4','.jpg','.jpeg','.png','.pdf','.gif','.webp','cifrados de extremo']

def clean(c):
    return '\n'.join(l for l in c.split('\n') if l.strip() and not any(p in l.lower() for p in SKIP))

# Usar los 5 chats más ricos en texto
BEST_CHATS = [
    "/Users/tomas/Desktop/compras_chats/extracted/Chat de WhatsApp con Dulce Recreo/Chat de WhatsApp con Dulce Recreo.txt",
    "/Users/tomas/Desktop/compras_chats/extracted/Chat de WhatsApp con Gonzalo/Chat de WhatsApp con Gonzalo.txt",
    "/Users/tomas/Desktop/compras_chats/extracted/Chat de WhatsApp con +54 9 280 400-0859/Chat de WhatsApp con +54 9 280 400-0859.txt",
    "/Users/tomas/Desktop/compras_chats/extracted/Chat de WhatsApp con Lihue/Chat de WhatsApp con Lihue.txt",
    "/Users/tomas/Desktop/compras_chats/extracted/WhatsApp Chat - +54 9 3816 98-8229/_chat.txt",
]

corpus = ""
for f in BEST_CHATS:
    name = os.path.basename(os.path.dirname(f))
    with open(f, encoding='utf-8', errors='replace') as fh:
        content = clean(fh.read())
    corpus += f"\n=== CHAT: {name} ===\n{content[:2500]}\n"
print(f"Corpus: {len(corpus)} chars")

CONTEXT = f"Analizás estas conversaciones de compradores de Vendedores Humanos (VH) — formación en ventas online USD 750-1000:\n{corpus}\n"
STRICT = "\n\nIMPORTANTE: Responde ÚNICAMENTE con las líneas en el formato exacto pedido. NADA de introducción, explicación, títulos con ###, ni texto adicional. Solo las líneas."

def ask(prompt, tokens=8192):
    full = CONTEXT + prompt + STRICT
    r = model.generate_content(full, generation_config={"max_output_tokens": tokens, "temperature": 1})
    text = re.sub(r'\*+', '', r.text.strip())
    # Remove markdown headers
    text = re.sub(r'^#+\s+.*$', '', text, flags=re.MULTILINE)
    return text

# ── HELPERS ───────────────────────────────────────────────────

def get_pipe_val(text, prefix):
    """Extrae valor de línea con PREFIX (y posible sufijo) seguido de | - : y valor."""
    for line in text.split('\n'):
        line = line.strip()
        # "PREFIX\w*: value" or "PREFIX\w*| value" or "PREFIX\w*- value"
        m = re.match(rf'^{re.escape(prefix)}\w*\s*[|\-:]\s*(.+)', line, re.IGNORECASE)
        if m: return m.group(1).strip().strip('"').strip("'")
        # "N- PREFIX\w*: value"
        m2 = re.match(rf'^\d+\s*[-–.)\s]+{re.escape(prefix)}\w*\s*[:\-|]\s*(.+)', line, re.IGNORECASE)
        if m2: return m2.group(1).strip().strip('"').strip("'")
    return ""

def parse_pipe(text, prefix, n_parts):
    """Extrae líneas con formato PREFIX<num>| p1 | p2 | ..."""
    items = []
    for line in text.split('\n'):
        line = line.strip()
        m = re.match(rf'^{re.escape(prefix)}\d*\s*[|\-:]\s*(.+)', line, re.IGNORECASE)
        if m:
            parts = [p.strip() for p in m.group(1).split('|')]
            while len(parts) < n_parts:
                parts.append('')
            items.append(parts[:n_parts])
    return items

def parse_pipe_all(text, prefix):
    """Extrae todas las líneas que empiezan con PREFIX seguido de número y |"""
    items = []
    for line in text.split('\n'):
        line = line.strip()
        m = re.match(rf'^{re.escape(prefix)}\d+\s*[|\-:]\s*(.+)', line, re.IGNORECASE)
        if m:
            items.append(m.group(1).strip())
    return items

# ── 1. PERFIL DEMOGRÁFICO ────────────────────────────────────
print("\n[1] Perfil demográfico...")
r1 = ask("""Describí el avatar comprador típico. Exactamente en este formato, una línea por campo:
NOMBRE: [nombre ficticio y apodo, ej: "Carla, la Ambiciosa"]
RESUMEN: [2-3 oraciones sobre quién es]
EDAD: [rango de edad, ej: "30-45 años"]
GENERO: [género predominante]
LABORAL: [situación laboral en 1 oración]
FAMILIAR: [situación familiar en 1 oración]
DIGITAL: [nivel digital: básico/intermedio/avanzado + descripción]
SITUACION: [su vida hoy antes de comprar VH, 2 oraciones]""")
with open('/tmp/r1.txt','w') as f: f.write(r1)
print(r1[:300])

# ── 2. DESEOS Y MOTIVACIONES ──────────────────────────────────
print("\n[2] Deseos y motivaciones...")
r2 = ask("""Describí los deseos y razones de compra del avatar de VH. Exactamente en este formato:
DESEO_DICHO: [lo que el cliente dice que quiere]
DESEO_REAL: [lo que realmente quiere]
DESEO_OCULTO: [el motor emocional nunca dicho en voz alta]
VISION_IDEAL: [cómo imagina su vida si todo sale bien]
RAZON_LOGICA: [justificación racional para comprar]
RAZON_EMOCIONAL: [razón emocional verdadera]
DETONANTE: [qué fue lo último que lo empujó a decir sí]""")
with open('/tmp/r2.txt','w') as f: f.write(r2)
print(r2[:300])

# ── 3. DOLORES ────────────────────────────────────────────────
print("\n[3] Dolores profundos...")
r3 = ask("""Listá los 5 DOLORES PROFUNDOS del avatar de VH. Para cada uno, exactamente en este formato (separado por |):
D1| [nombre del dolor] | [descripción 1 oración] | [frase textual corta del chat]
D2| [nombre] | [descripción] | [frase]
D3| [nombre] | [descripción] | [frase]
D4| [nombre] | [descripción] | [frase]
D5| [nombre] | [descripción] | [frase]""")
with open('/tmp/r3.txt','w') as f: f.write(r3)
print(r3[:300])

# ── 4. MIEDOS ─────────────────────────────────────────────────
print("\n[4] Miedos específicos...")
r4 = ask("""Listá los 5 MIEDOS ESPECÍFICOS del avatar de VH. Para cada uno exactamente en este formato:
M1| [nombre del miedo] | [cómo se manifiesta] | [frase textual del chat]
M2| [nombre] | [manifestación] | [frase]
M3| [nombre] | [manifestación] | [frase]
M4| [nombre] | [manifestación] | [frase]
M5| [nombre] | [manifestación] | [frase]""")
with open('/tmp/r4.txt','w') as f: f.write(r4)
print(r4[:300])

# ── 5. CREENCIAS ─────────────────────────────────────────────
print("\n[5] Creencias limitantes...")
r5a = ask("""Listá las 4 CREENCIAS LIMITANTES del avatar de VH. Exactamente este formato:
C1| [creencia] | [origen de esta creencia] | [cómo la bloquea]
C2| [creencia] | [origen] | [cómo bloquea]
C3| [creencia] | [origen] | [cómo bloquea]
C4| [creencia] | [origen] | [cómo bloquea]""")
with open('/tmp/r5a.txt','w') as f: f.write(r5a)
print(r5a[:300])

# ── 6. FRUSTRACIONES Y VERGÜENZAS ────────────────────────────
print("\n[6] Frustraciones y vergüenzas...")
r5b = ask("""Describí las frustraciones cotidianas y vergüenzas del avatar de VH.

FRUSTRACIONES (5 frustraciones del día a día, exactamente este formato):
F1| [frustración concreta]
F2| [frustración]
F3| [frustración]
F4| [frustración]
F5| [frustración]

VERGÜENZAS (4 cosas que sienten pero nunca dicen):
V1| [vergüenza o pensamiento no dicho]
V2| [vergüenza]
V3| [vergüenza]
V4| [vergüenza]""")
with open('/tmp/r5b.txt','w') as f: f.write(r5b)
print(r5b[:300])

# ── 7. LENGUAJE PROPIO ────────────────────────────────────────
print("\n[7] Frases textuales del cliente...")
r6a = ask("""Copiá 8 frases TEXTUALES de los chats que usan los clientes de VH (tal cual aparecen en los chats):
FL1| [frase textual copiada del chat]
FL2| [frase]
FL3| [frase]
FL4| [frase]
FL5| [frase]
FL6| [frase]
FL7| [frase]
FL8| [frase]""")
with open('/tmp/r6a.txt','w') as f: f.write(r6a)
print(r6a[:300])

# ── 8. FACTORES DE CONFIANZA ─────────────────────────────────
print("\n[8] Factores de confianza...")
r6b = ask("""Listá los 5 factores que generaron CONFIANZA en VH según los chats:
CT1| [factor de confianza]
CT2| [factor]
CT3| [factor]
CT4| [factor]
CT5| [factor]""")
with open('/tmp/r6b.txt','w') as f: f.write(r6b)
print(r6b[:200])

# ── 9. ESTRATEGIA DE LANZAMIENTO ──────────────────────────────
print("\n[9] Estrategia de lanzamiento...")
r7 = ask("""Diseñá la estrategia de comunicación para el próximo lanzamiento de VH:

MENSAJE_CENTRAL| [la promesa que más resuena, 1-2 oraciones impactantes]
OFERTA_IDEAL| [estructura de precio/cuotas/garantía que funcionó mejor]

ANGULOS (4 ángulos de comunicación, exactamente este formato):
ANG1| [nombre del ángulo] | [descripción] | [copy de venta 2 oraciones] | [cuándo usar]
ANG2| [nombre] | [descripción] | [copy] | [cuándo]
ANG3| [nombre] | [descripción] | [copy] | [cuándo]
ANG4| [nombre] | [descripción] | [copy] | [cuándo]

PALABRAS PROHIBIDAS (5 frases que generan rechazo):
PRH1| [palabra o frase a evitar]
PRH2| [palabra]
PRH3| [palabra]
PRH4| [palabra]
PRH5| [palabra]

PALABRAS PODEROSAS (8 palabras que resonaron positivamente):
POD1| [palabra]
POD2| [palabra]
POD3| [palabra]
POD4| [palabra]
POD5| [palabra]
POD6| [palabra]
POD7| [palabra]
POD8| [palabra]""")
with open('/tmp/r7.txt','w') as f: f.write(r7)
print(r7[:400])

# ── 10. INSIGHTS OCULTOS ───────────────────────────────────────
print("\n[10] Insights ocultos...")
r8 = ask("""Listá 5 INSIGHTS OCULTOS no obvios del comportamiento de compradores de VH:
IN1| [título impactante] | [descripción del hallazgo] | [qué hacer con esto] | [frase real del chat]
IN2| [título] | [descripción] | [acción] | [frase]
IN3| [título] | [descripción] | [acción] | [frase]
IN4| [título] | [descripción] | [acción] | [frase]
IN5| [título] | [descripción] | [acción] | [frase]""")
with open('/tmp/r8.txt','w') as f: f.write(r8)
print(r8[:300])

# ── 11. ETAPAS DE COMPRA ─────────────────────────────────────
print("\n[11] Etapas del proceso de compra...")
r9a = ask("""Describí las 4 etapas del proceso de compra de VH. Exactamente este formato:
ET1| [nombre etapa] | [descripción] | [duración típica] | [acciones vendedor separadas por ;] | [frase textual del chat]
ET2| [nombre] | [descripción] | [duración] | [acciones] | [frase]
ET3| [nombre] | [descripción] | [duración] | [acciones] | [frase]
ET4| [nombre] | [descripción] | [duración] | [acciones] | [frase]""")
with open('/tmp/r9a.txt','w') as f: f.write(r9a)
print(r9a[:300])

# ── 12. OBJECIONES + TRIGGERS ─────────────────────────────────
print("\n[12] Objeciones y triggers...")
r9b = ask("""Listá las 5 objeciones más frecuentes de clientes de VH y los 5 triggers de compra:

OBJECIONES (exactamente este formato):
OBJ1| [nombre objeción] | [alta/media/baja] | [frase textual del cliente] | [respuesta efectiva del vendedor]
OBJ2| [nombre] | [frecuencia] | [frase cliente] | [respuesta]
OBJ3| [nombre] | [frecuencia] | [frase] | [respuesta]
OBJ4| [nombre] | [frecuencia] | [frase] | [respuesta]
OBJ5| [nombre] | [frecuencia] | [frase] | [respuesta]

TRIGGERS (exactamente este formato):
TRG1| [trigger de compra]
TRG2| [trigger]
TRG3| [trigger]
TRG4| [trigger]
TRG5| [trigger]

MEJORES PRACTICAS (5 prácticas que funcionaron):
MP1| [práctica concreta]
MP2| [práctica]
MP3| [práctica]
MP4| [práctica]
MP5| [práctica]""")
with open('/tmp/r9b.txt','w') as f: f.write(r9b)
print(r9b[:400])

# ── 13. ANÁLISIS DE VENDEDORES ────────────────────────────────
print("\n[13] Análisis de vendedores...")
r10 = ask("""Identificá a los vendedores de VH que aparecen en los chats y analizalos:
VD1| [nombre vendedor] | [estilo de venta] | [fortaleza1;fortaleza2;fortaleza3] | [mejora1;mejora2] | [tasa cierre estimada]
VD2| [nombre] | [estilo] | [fortalezas separadas por ;] | [mejoras separadas por ;] | [tasa]
VD3| [nombre] | [estilo] | [fortalezas] | [mejoras] | [tasa]

FRASES VENDEDOR QUE FUNCIONARON (8 frases textuales que usaron los vendedores):
FV1| [frase textual del vendedor]
FV2| [frase]
FV3| [frase]
FV4| [frase]
FV5| [frase]
FV6| [frase]
FV7| [frase]
FV8| [frase]

SEÑALES DE INTERES (6 frases del cliente que indican que está listo para comprar):
SI1| [frase]
SI2| [frase]
SI3| [frase]
SI4| [frase]
SI5| [frase]
SI6| [frase]""")
with open('/tmp/r10.txt','w') as f: f.write(r10)
print(r10[:400])

print("\nProcesando respuestas...")

# ── PARSEO FINAL ──────────────────────────────────────────────

# Perfil (r1 usa NOMBRE:, RESUMEN:, EDAD:, etc.)
demo = {
    "nombre_ficticio": get_pipe_val(r1, "NOMBRE"),
    "resumen": get_pipe_val(r1, "RESUMEN"),
    "edad_rango": get_pipe_val(r1, "EDAD"),
    "genero_predominante": get_pipe_val(r1, "GENERO"),
    "situacion_laboral": get_pipe_val(r1, "LABORAL"),
    "situacion_familiar": get_pipe_val(r1, "FAMILIAR"),
    "nivel_digital": get_pipe_val(r1, "DIGITAL"),
    "situacion_actual": get_pipe_val(r1, "SITUACION"),
}

# Deseos (r2)
deseos = {
    "deseo_superficial": get_pipe_val(r2, "DESEO_DICHO"),
    "deseo_real": get_pipe_val(r2, "DESEO_REAL"),
    "deseo_oculto": get_pipe_val(r2, "DESEO_OCULTO"),
    "vision_vida_ideal": get_pipe_val(r2, "VISION_IDEAL") or get_pipe_val(r2, "VISION"),
    "razon_logica": get_pipe_val(r2, "RAZON_LOGICA") or get_pipe_val(r2, "RAZON"),
    "razon_emocional": get_pipe_val(r2, "RAZON_EMOCIONAL"),
    "detonante_final": get_pipe_val(r2, "DETONANTE"),
}

# Dolores D1| nombre | desc | frase
dolores = []
for parts in parse_pipe(r3, "D", 3):
    if parts[0]:
        dolores.append({"dolor": parts[0], "descripcion": parts[1], "evidencia_real": parts[2]})

# Miedos M1| nombre | manifestación | frase
miedos = []
for parts in parse_pipe(r4, "M", 3):
    if parts[0]:
        miedos.append({"miedo": parts[0], "descripcion": parts[1], "evidencia_real": parts[2]})

# Creencias C1| creencia | origen | como_bloquea
creencias = []
for parts in parse_pipe(r5a, "C", 3):
    if parts[0]:
        creencias.append({"creencia": parts[0], "origen": parts[1], "como_bloquea": parts[2]})

frustraciones = parse_pipe_all(r5b, "F")
vergüenzas = parse_pipe_all(r5b, "V")
frases_propias = parse_pipe_all(r6a, "FL")
factores_confianza = parse_pipe_all(r6b, "CT")

# Estrategia (r7)
mensaje_central = get_pipe_val(r7, "MENSAJE_CENTRAL") or get_pipe_val(r7, "MENSAJE")
oferta = get_pipe_val(r7, "OFERTA_IDEAL") or get_pipe_val(r7, "OFERTA")
angulos = []
for parts in parse_pipe(r7, "ANG", 4):
    if parts[0]:
        angulos.append({"angulo": parts[0], "descripcion": parts[1], "copy": parts[2], "cuando_usar": parts[3]})
prohibidas = parse_pipe_all(r7, "PRH")
poderosas = parse_pipe_all(r7, "POD")

# Insights IN1| título | desc | accion | frase
insights = []
for parts in parse_pipe(r8, "IN", 4):
    if parts[0]:
        insights.append({"insight": parts[0], "descripcion": parts[1], "implicancia": parts[2], "evidencia": parts[3]})

# Etapas ET1| nombre | desc | dur | acciones | frase
etapas = []
for i, parts in enumerate(parse_pipe(r9a, "ET", 5)):
    if parts[0]:
        etapas.append({
            "numero": str(i+1),
            "nombre": parts[0],
            "descripcion": parts[1],
            "duracion_tipica": parts[2],
            "acciones_vendedor": [a.strip() for a in parts[3].split(';')] if parts[3] else [],
            "ejemplos_reales": [parts[4]] if parts[4] else [],
        })

# Objeciones OBJ1| nombre | freq | frase | respuesta
objeciones = []
for parts in parse_pipe(r9b, "OBJ", 4):
    if parts[0]:
        objeciones.append({
            "objecion": parts[0],
            "frecuencia": parts[1],
            "ejemplos_reales": [parts[2]] if parts[2] else [],
            "respuestas_efectivas": [parts[3]] if parts[3] else [],
        })

triggers = parse_pipe_all(r9b, "TRG")
practicas = parse_pipe_all(r9b, "MP")

# Vendedores VD1| nombre | estilo | fortalezas | mejoras | cierre
vendedores = []
for parts in parse_pipe(r10, "VD", 5):
    if parts[0]:
        vendedores.append({
            "vendedor": parts[0],
            "estilo": parts[1],
            "fortalezas": [f.strip() for f in parts[2].split(';')] if parts[2] else [],
            "areas_mejora": [m.strip() for m in parts[3].split(';')] if parts[3] else [],
            "tasa_cierre_estimada": parts[4],
        })

frases_vendedor = parse_pipe_all(r10, "FV")
señales_interes = parse_pipe_all(r10, "SI")

# ── GUARDAR ───────────────────────────────────────────────────
final = {
    "meta": {"total_chats": 18, "fecha_analisis": "Abril 2026"},
    "resumen_ejecutivo": demo.get("resumen", ""),
    "avatar_principal": {
        "nombre_ficticio": demo.get("nombre_ficticio", ""),
        "datos_demograficos": {
            "edad_rango": demo.get("edad_rango", ""),
            "genero_predominante": demo.get("genero_predominante", ""),
            "situacion_laboral": demo.get("situacion_laboral", ""),
            "situacion_familiar": demo.get("situacion_familiar", ""),
            "nivel_digital": demo.get("nivel_digital", ""),
        },
        "situacion_actual": demo.get("situacion_actual", ""),
        "deseo_superficial": deseos.get("deseo_superficial", ""),
        "deseo_real": deseos.get("deseo_real", ""),
        "deseo_oculto": deseos.get("deseo_oculto", ""),
        "vision_vida_ideal": deseos.get("vision_vida_ideal", ""),
        "razon_logica": deseos.get("razon_logica", ""),
        "razon_emocional": deseos.get("razon_emocional", ""),
        "detonante_final": deseos.get("detonante_final", ""),
        "dolores_profundos": dolores,
        "miedos_especificos": miedos,
        "creencias_limitantes": creencias,
        "frustraciones_cotidianas": frustraciones,
        "vergüenzas_no_dichas": vergüenzas,
        "palabras_propias": frases_propias,
        "factores_confianza": factores_confianza,
    },
    "estrategia_lanzamiento": {
        "mensaje_central": mensaje_central,
        "angulos": angulos,
        "palabras_prohibidas": prohibidas,
        "palabras_poderosas": poderosas,
        "oferta_ideal": oferta,
    },
    "insights_ocultos": insights,
    "etapas_proceso_compra": etapas,
    "objeciones_frecuentes": objeciones,
    "triggers_de_compra": triggers,
    "mejores_practicas_detectadas": practicas,
    "patrones_linguisticos": {
        "frases_vendedor_que_funcionaron": frases_vendedor,
        "frases_cliente_que_indican_interes": señales_interes,
    },
    "analisis_por_vendedor": vendedores,
    "metricas_proceso": {"total_chats_analizados": 18},
}

out = "/Users/tomas/Feedback Alumnos VH (Claude)/feedback-vendedores/chat_analysis.json"
with open(out, 'w', encoding='utf-8') as f:
    json.dump(final, f, ensure_ascii=False, indent=2)

av = final["avatar_principal"]
print(f"\n✓ GUARDADO en {out}")
print(f"  Avatar: {av['nombre_ficticio']}")
print(f"  Resumen: {final['resumen_ejecutivo'][:100]}")
print(f"  Deseo oculto: {av['deseo_oculto'][:80]}")
print(f"  Dolores: {len(av['dolores_profundos'])}")
print(f"  Miedos: {len(av['miedos_especificos'])}")
print(f"  Creencias: {len(av['creencias_limitantes'])}")
print(f"  Frustraciones: {len(av['frustraciones_cotidianas'])}")
print(f"  Vergüenzas: {len(av['vergüenzas_no_dichas'])}")
print(f"  Frases propias: {len(av['palabras_propias'])}")
print(f"  Factores confianza: {len(av['factores_confianza'])}")
print(f"  Ángulos lanzamiento: {len(final['estrategia_lanzamiento']['angulos'])}")
print(f"  Palabras prohibidas: {len(final['estrategia_lanzamiento']['palabras_prohibidas'])}")
print(f"  Palabras poderosas: {len(final['estrategia_lanzamiento']['palabras_poderosas'])}")
print(f"  Insights ocultos: {len(final['insights_ocultos'])}")
print(f"  Etapas proceso: {len(final['etapas_proceso_compra'])}")
print(f"  Objeciones: {len(final['objeciones_frecuentes'])}")
print(f"  Triggers: {len(final['triggers_de_compra'])}")
print(f"  Mejores prácticas: {len(final['mejores_practicas_detectadas'])}")
print(f"  Vendedores: {len(final['analisis_por_vendedor'])}")
print(f"  Frases vendedor: {len(final['patrones_linguisticos']['frases_vendedor_que_funcionaron'])}")
print(f"  Señales interés: {len(final['patrones_linguisticos']['frases_cliente_que_indican_interes'])}")
