import logging

import anthropic

import config

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"


def generate_feedback(vendor_name: str, transcript: str, criteria: str) -> str:
    """
    Generate personalized sales coaching feedback using Claude.
    Returns the feedback as a formatted string.
    Raises an exception on failure.
    """
    if not config.ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured.")

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    language = config.FEEDBACK_LANGUAGE

    system_prompt = f"""Sos un coach experto en la metodología de Ventas Humanas (VH), inspirada en los principios de Dale Carnegie ("Cómo ganar amigos e influir sobre las personas"), Jürgen Klaric ("Véndele a la mente, no a la gente") y Robert Greene ("Naturaleza Humana").

Tu misión es dar feedback honesto, humano y accionable a vendedores que están aprendiendo esta metodología. El potencial cliente nunca debe sentirse presionado: la venta es una consecuencia natural de una conversación genuina.

La estructura de la venta VH tiene 4 etapas (usá siempre este vocabulario exacto):
1. DESCUBRIMIENTO — la etapa más importante. Incluye: Rapport, Claridad, Tomador de Decisión, Situación Actual, Situación Deseada, GAP, los 5 Acuerdos, Desafíos, Dolores, Costo de Oportunidad, Preferencia, Urgencia, Compromiso y Venta Inversa.
2. DIAGNÓSTICO — qué solución necesita el lead y cuál es el camino personalizado.
3. PERMISO — transición al pitch, siempre pidiendo autorización.
4. PITCH — presentación de pilares conectados a los dolores específicos del lead, con certeza y seguridad.

SISTEMA DE FEEDBACK — SCI + PRÓXIMO PASO:
Cada punto de fortaleza o mejora debe seguir este formato:
"Cuando [situación concreta de la llamada], [conducta específica del vendedor], [impacto que eso tuvo en el lead o en la venta]. La próxima vez podés [próximo paso accionable]."

Reglas:
- Usá lenguaje natural, cálido y directo. Nada de jerga corporativa.
- Citá frases textuales o momentos concretos de la transcripción siempre que sea posible.
- No te limites a un número fijo de puntos: marcá TODOS los que sean relevantes, tanto fortalezas como mejoras.
- Usá siempre los términos VH: Descubrimiento, Diagnóstico, Permiso, Pitch. No "propuesta", no "cierre", no "apertura".
- Escribís siempre en {language}.

PLAN DE ACCIÓN — LINKS A CLASES:
Al final del feedback, en la sección "Plan de Acción", incluí siempre 2 a 4 clases específicas del curso que el vendedor debería repasar según sus puntos débiles. Usá exactamente este formato para cada clase recomendada:

[▶ Nombre de la clase](http://localhost:5000/formacion#modX)

Donde X es el número del módulo correspondiente. Mapa de módulos disponibles:

| Módulo | Contenido | Link |
|--------|-----------|------|
| mod1 | Onboarding: qué es VH, uso de la plataforma, días y horarios | http://localhost:5000/formacion#mod1 |
| mod2 | Misión 1 (ejercicio inicial) | http://localhost:5000/formacion#mod2 |
| mod3 | Sistema de Ventas Humanas: estructura completa, 13 pasos, objeciones | http://localhost:5000/formacion#mod3 |
| mod4 | Psicología y Neuroventas: cerebro, historias, sesgos, hombres vs. mujeres, 6 errores | http://localhost:5000/formacion#mod4 |
| mod5 | Seguimientos Creativos: 4 tipos de seguimiento | http://localhost:5000/formacion#mod5 |
| mod6 | Mentalidad: patrones de éxito, creatividad, alterego, VAKOG | http://localhost:5000/formacion#mod6 |

Elegí las clases más relevantes para los puntos de mejora detectados en esa llamada específica. No las incluyas todas — solo las que realmente apliquen."""

    user_prompt = f"""Analizá la siguiente transcripción de una llamada de ventas realizada por **{vendor_name}** y generá un feedback completo basado en la metodología VH.

## CRITERIOS DE EVALUACIÓN
{criteria}

## TRANSCRIPCIÓN
{transcript}

---

Estructurá el feedback exactamente así:

---

### Feedback para {vendor_name}

**Puntuación general:** [X/10] — [una frase que resuma el desempeño overall de forma honesta y humana]

---

### 1. DESCUBRIMIENTO ⭐
*(Esta es la etapa más importante de la venta. Analizala en profundidad.)*

#### Fortalezas en el Descubrimiento
[Usá el formato SCI + Próximo Paso para cada punto. Marcá TODOS los que correspondan, sin límite.]

#### Puntos de mejora en el Descubrimiento
[Usá el formato SCI + Próximo Paso para cada punto. Marcá TODOS los que correspondan, sin límite.]

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
*(Completar solo si hubo objeciones en la llamada. Si no las hubo, indicarlo.)*

**Nivel de certeza y seguridad al manejar objeciones:** [Alto / Medio / Bajo] — [observación concreta]

#### Fortalezas
[SCI + Próximo Paso.]

#### Puntos de mejora
[SCI + Próximo Paso.]

---

### 6. ESCUCHA ACTIVA, COMUNICACIÓN Y PSICOLOGÍA DE VENTAS
[Evaluá aquí: escucha activa real vs. seguir un guion, parafraseo con las palabras exactas del lead, profundizar respuestas abstractas, creatividad y metáforas, igualación de tono y ritmo, desapego, manejo del ego del cliente, silencio después del precio, terminar intervenciones con una pregunta, y cualquier otro punto de comunicación relevante. Usá SCI + Próximo Paso para cada observación.]

---

### Mensaje final
[2-3 oraciones de cierre, humanas y personalizadas para {vendor_name}. Reconocé el esfuerzo, señalá el mayor logro de esta llamada y dejalo motivado para la próxima.]

---

### 🎯 Plan de Acción — Clases a Repasar
*Basado en los puntos de mejora de esta llamada, estas son las clases que te recomiendo repasar:*

[Listá 2-4 clases usando el formato: [▶ Nombre de la clase](http://localhost:5000/formacion#modX) — una línea de explicación de por qué esa clase aplica a lo que se detectó en esta llamada.]"""

    logger.info("Generating feedback for vendor: %s (transcript length: %d chars)", vendor_name, len(transcript))

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
        )
    except anthropic.APIError as exc:
        raise RuntimeError(f"Anthropic API error: {exc}") from exc

    feedback_text = message.content[0].text
    logger.info("Feedback generated successfully. Length: %d chars", len(feedback_text))
    return feedback_text
