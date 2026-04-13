import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
PRODUCER_PASSWORD = os.getenv("PRODUCER_PASSWORD", "vh2026")
FEEDBACK_LANGUAGE = os.getenv("FEEDBACK_LANGUAGE", "español")
YOUR_NAME = os.getenv("YOUR_NAME", "Tu director comercial")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "5120"))

_DEFAULT_CRITERIA = """
Evaluá esta llamada según la metodología de VENTAS HUMANAS (VH).

Las 4 etapas de la venta son (usá siempre estos nombres exactos): Descubrimiento, Diagnóstico, Permiso y Pitch.

Esta metodología está inspirada en:
- Dale Carnegie: "Cómo ganar amigos e influir sobre las personas" (el cliente nunca debe sentirse presionado, siempre debe sentirse escuchado, comprendido e importante)
- Jürgen Klaric: "Véndele a la mente, no a la gente" (neuroventas, cerebro reptil, emocional y racional)
- Robert Greene: "Naturaleza Humana" (entender los motivadores profundos del comportamiento humano)

---

### 1. RAPPORT
El rapport real NO es hablar del clima, hacer comentarios vacíos ni relleno innecesario. El rapport genuino se construye durante TODA la llamada, no solo al inicio, y se genera conectando con los sesgos psicológicos del lead.

**Lo que hace un buen rapport:**
- Buscar puntos en común con el lead para que se sienta familiar con el vendedor ("¿también estás en el mundo X?", notar coincidencias reales y mencionarlas).
- Hacer sentir al lead importante: escucharlo de verdad, recordar lo que dijo y retomarlo más adelante.
- Hacer sentir al lead inteligente: validar sus análisis y decisiones previas sin adular en exceso.
- Hacer sentir al lead especial: tratarlo como un caso único, no como un prospecto más.
- Preguntar por las pasiones del lead (familia, proyectos personales, intereses) y conectar genuinamente con ellas.
- Igualación de energía: el vendedor adapta su ritmo y tono al del lead, nunca al revés.

**Señales de mal rapport:**
- Hablar del clima, el día o temas genéricos sin conexión real con la persona.
- Halagos excesivos o forzados ("¡qué buenísima pregunta!").
- Rapport solo al inicio y luego cero conexión humana durante el resto de la llamada.
- El vendedor habla más de sí mismo que del lead.

**Preguntas para evaluar:**
- ¿El lead se sintió escuchado, comprendido e importante en algún momento de la llamada?
- ¿El vendedor encontró algún punto en común genuino y lo mencionó?
- ¿Preguntó por las pasiones o intereses del lead?
- ¿El rapport se mantuvo vivo durante toda la llamada o desapareció después del saludo?

---

### 2. CLARIDAD (sacar la presión de venta)
- ¿Comunicó el propósito de la reunión de forma clara y directa?
- ¿Le dio al lead la sensación de que se lo va a ayudar a tener claridad, no a venderle?

---

### 3. TOMADOR DE DECISIÓN
- ¿Preguntó si alguien más debería estar en la reunión para tomar la decisión?

---

### 4. DESCUBRIMIENTO — Situación Actual
- ¿Hizo preguntas para entender dónde está el lead hoy (ingresos, clientes, negocio, tiempo con el negocio)?
- ¿Escuchó activamente y re-preguntó para no quedarse con lo superficial?
- ¿Volvió a preguntar usando las mismas palabras que el lead dijo? (Eso genera que el lead quiera dar más información.)
- ¿Usó los mismos términos y palabras que el cliente?
- ¿Evitó hablar de la solución, aportar valor o decir "te puedo ayudar" en esta etapa?

---

### 5. DESCUBRIMIENTO — Situación Deseada y GAP
- ¿Preguntó sobre la meta, el porqué de esa meta y el estilo de vida que el lead busca?
- ¿Generó GAP (distancia emocional/económica entre situación actual y deseada)?
- ¿Cuantificó el GAP con el lead? (Objetivo anual − situación actual = costo por no tener la solución.)
- ¿Ancló al lead a su meta haciéndole sentir el valor de lograrla?

---

### 6. DESCUBRIMIENTO — 5 Acuerdos de VH
Verificar si se llegó a los siguientes acuerdos (el cliente los confirma, el vendedor solo pregunta):
- **Acuerdo 1:** El prospecto quiere mejores resultados.
- **Acuerdo 2:** Obtener ese resultado es posible.
- **Acuerdo 3:** Hay un problema específico que le impide tener ese resultado.
- **Acuerdo 4:** Ese problema le está costando dinero (costo de oportunidad cuantificado).
- **Acuerdo 5:** No puede resolverlo solo. ("¿Qué estás haciendo para cambiar esto?")

---

### 7. DESAFÍOS Y DOLORES DEL CLIENTE
- ¿Indagó más allá del primer dolor? ¿Buscó dolores adicionales e indirectos?
- ¿Los problemas los mencionó el cliente o el vendedor los instaló?
- ¿Ancló los problemas a emociones? ("¿Cómo te sentís en lo personal? ¿Te frustra, te estresa?")
- ¿Descubrió el costo hundido? ("¿Hace cuánto tenés este problema?" / "¿Qué intentaste antes que no funcionó?")
- ¿Preguntó si el problema afecta otras áreas de la vida (relaciones, familia, salud)?
- ¿Cuantificó el costo: tiempo, dinero o salud?
- ¿Usó preguntas de proyección? ("¿Qué creés que pasaría si seguís 1 año más así?" / "¿Podrías continuar 3 años más así?")

---

### 8. VENTA INVERSA, PREFERENCIA, URGENCIA Y COMPROMISO
- ¿Usó preguntas de venta inversa para que el cliente se convenza a sí mismo?
  - "¿Por qué sentís que serías un buen candidato para este programa?"
  - "¿Por qué lo harías con [vendedor] y no con otro referente?"
  - "¿Dónde creés que te va a ayudar en tu caso particular?"
- ¿Preguntó por qué preferiría un acompañamiento vs. hacerlo solo?
- ¿Preguntó para cuándo quiere el problema resuelto y qué pasa si no decide pronto?
- ¿Confirmó el nivel de compromiso del lead (escala del 1 al 10)?

---

### 9. PERMISO (transición al pitch)
- ¿Preguntó si había algo más que no se hubiera abordado antes de avanzar?
- ¿Hizo un repaso opcional de los puntos clave hablados?
- ¿Pidió permiso explícito para pasar al pitch?
- ¿Confirmó compromiso antes de pitchear ("¿Del 1 al 10, qué tan comprometido estás?")?

---

### 10. DIAGNÓSTICO
- ¿Explicó qué tiene que hacer el lead para salir de su situación actual?
- ¿Usó términos o pasos que el lead no pueda resolver por su cuenta o que le generen curiosidad?

---

### 11. PITCH
- ¿Ancló cada pilar/paso de la oferta al dolor/deseo específico del lead?
- ¿Siguió la estructura: problema del lead → solución/característica → confirmación conjunta?
- ¿Apuntó a los miedos del lead (leyéndolos entre líneas si no los dijo explícitamente)?
- ¿Usó storytelling con historia del héroe para que el lead se identifique?
- ¿Hizo check-ins entre pilares ("¿alguna duda?", "¿venís bien?", "¿te hace sentido?", "¿dudas?")?
- ¿Esperó a que el cliente preguntara el precio? Si no lo hizo, ¿preguntó sutilmente qué le gustaría hacer?
- Antes del precio: ¿preguntó si había algo más que no se hubiera abordado?
- ¿Dio el precio sin justificarlo? (La justificación está en todo lo anterior.)
- ¿Evitó palabras prohibidas: "podría", "mejorar", "curso", "aprender", "creo"?

---

### 12. ESCUCHA ACTIVA, LENGUAJE Y PERSONALIZACIÓN
- ¿Usó los mismos términos que el lead a lo largo de toda la llamada?
- ¿Demostró que escuchó conectando los pilares del pitch a las palabras exactas del cliente?
- ¿Pidió permiso antes de hacer preguntas íntimas o personales?

---

### 13. MANEJO DE OBJECIONES
Evaluar si hubo objeciones y cómo se manejaron según la metodología VH.

**Regla clave del checklist VH:** Nunca resolver la primera objeción sin antes verificar que es la única.
- "Quitando el tema dinero, ¿hay alguna otra razón por la que no lo harías, o solo es eso?"
- "Si no fuera que lo tenés que consultar con tu esposa, ¿hay algo más que te impide tomar esta decisión?"

**Principios que debe aplicar:**
- ¿Mantuvo la calma y NO confrontó al lead? (Confrontar cambia la emoción y el cliente quiere escapar.)
- ¿Trató la objeción como una creencia limitante o inseguridad, no como un ataque personal?
- ¿Usó la objeción para aumentar la certeza del cliente, no para discutir?
- ¿Después de responder la objeción, volvió a pedir la venta? ("¿Qué te gustaría que hagamos, Juan?")

**Objeciones comunes y cómo debería haberlas manejado:**
- **"Lo tengo que pensar"** → Preguntar qué tiene que pensar exactamente. Buscar la duda real detrás.
- **"Mandame la info por email"** → Preguntar qué información falta para empezar hoy. Ser directo sobre que generalmente significa un NO.
- **"Es muy caro"** → Volver al objetivo y al dolor: si resuelve [objetivo], ¿importa si cuesta X o Y?
- **"Lo tengo que hablar con mi pareja/socio"** → Verificar si es lo único que falta. Si insiste, agendar reunión con ambos.
- **"No tengo tiempo"** → Reencuadrar como cuestión de prioridades. Recordar el costo de oportunidad.
- **"No tengo todo el dinero"** → Nunca resolver lo económico sin asegurar certeza en la solución primero. Explorar reserva o plan de pagos.
- **"No tomo decisiones rápidas"** → Ayudar al cliente a llegar a su propia conclusión: decisiones del pasado = resultados actuales.

**Señales de mal manejo de objeciones:**
- Bajar el precio o ceder ante la primera resistencia.
- Ponerse defensivo o argumentar agresivamente.
- No volver a pedir la venta después de resolver la objeción.
- Aceptar "después te aviso" sin indagar qué falta realmente.

---

### 13. NEUROVENTAS — Activación de los 3 cerebros
Verificar si el vendedor activó los tres niveles de decisión:

**Cerebro reptil (70-80% de la decisión):**
- ¿Apuntó al placer inmediato o al alejamiento del dolor/miedo?
- ¿Usó lenguaje simple, tangible y concreto?
- ¿Transmitió ahorro de tiempo y energía?

**Cerebro emocional (20-30% de la decisión):**
- ¿Hizo sentir al cliente único, especial, comprendido?
- ¿Se conectó como persona, no como robot vendedor?
- ¿Usó el nombre del cliente? ¿Validó sus opiniones?
- ¿Mostró interés genuino en la persona y sus problemas?

**Cerebro racional (justificación lógica):**
- ¿Aportó datos, hechos, garantías o casos de personas similares para que el cliente justifique la decisión?

**Error clave a detectar:** ¿El vendedor habló solo del producto en lugar de conectarlo con cómo ayuda al cliente?

---

### 14. NEUROVENTAS — Herramientas avanzadas
- **¿Usó historias?** ¿Tenían personaje identificable, problema real, mentor/solución y resultado concreto? ¿Eran específicas y detalladas?
- **¿Usó contraste?** ¿Comparó la situación actual del lead con lo que podría tener?
- **¿Activó aversión a las pérdidas?** ¿Destacó lo que el cliente pierde por no actuar?
- **¿Resaltó victorias rápidas?** ¿Qué gana el cliente a corto plazo?
- **¿Usó sesgos de forma ética?**
  - Costo hundido: ¿Recordó al cliente lo que ya invirtió (tiempo, dinero, esfuerzo)?
  - Sesgo de certeza: ¿Habló con convicción y claridad sobre los resultados esperados?
  - Sesgo de autoridad: ¿Transmitió pasión genuina por el servicio?
  - Sesgo de retrospectiva: ¿Usó momentos del pasado donde el cliente no tomó una oportunidad?

---

### 15. ERRORES GENERALES A DETECTAR
- ¿Habló demasiado de sí mismo o de su producto en lugar de centrarse en el cliente?
- ¿La llamada superó los 60 minutos sin necesidad? (El discurso se vuelve insostenible.)
- ¿Discutió o contradijo al cliente en algún momento?
- ¿Adaptó su comunicación al perfil del cliente? (Hombres: directo, datos, resultado. Mujeres: proceso, emoción, relación a largo plazo.)
- ¿Hizo doble o triple pregunta al mismo tiempo? (Error: el cliente no sabe cuál responder.)
- ¿Ofreció planes de cuotas o financiación sin que el cliente lo solicitara? (Nunca hacerlo proactivamente.)

---

### 16. COMUNICACIÓN AVANZADA Y PSICOLOGÍA DE VENTAS

**Terminar siempre con una pregunta:**
- En todo el proceso, ¿el vendedor terminó sus intervenciones con una pregunta al cliente? Esto es fundamental para mantener el control de la conversación y evitar que el cliente quede en silencio o se "desconecte".

**Parafraseo y vocabulario exacto del cliente:**
- ¿Usó las palabras EXACTAS del cliente para referirse a sus dolores? (Si el cliente dijo "me siento estancado", el vendedor debe decir "ese estancamiento", no "tu falta de progreso".)
- ¿Usó los mismos valores y términos que el cliente usa para describir lo que busca?

**Profundizar respuestas abstractas:**
- ¿Cuando el cliente dio una respuesta vaga o abstracta ("quiero escalar", "quiero más libertad"), el vendedor preguntó qué significa eso para esa persona en particular?
- No quedarse jamás con la primera respuesta superficial.

**Conexión personal y desvíos estratégicos:**
- ¿El vendedor generó momentos de conexión genuina más allá de la venta (preguntas personales, interés real por la persona)? Esto es positivo: el cliente debe sentirse escuchado, comprendido e importante, no que le están queriendo vender.

**Silencio después del precio:**
- ¿Después de decir el precio, el vendedor se quedó en silencio hasta que el cliente habló? Romper ese silencio es uno de los errores más costosos.

**Igualación de tonalidad y ritmo:**
- ¿El vendedor habló a la misma velocidad y tono que el cliente? Si el cliente es lento y reflexivo y el vendedor es rápido y ansioso, se rompe la confianza de forma subconsciente.

**Escucha "entre líneas":**
- ¿El vendedor identificó la emoción real detrás de lo que el cliente dijo? (Ejemplo: "no tengo tiempo" puede significar "tengo miedo de fallar otra vez".) ¿Trabajó sobre esa emoción real o se quedó en la superficie?

**Manejo del ego del cliente:**
- ¿El cliente se sintió el héroe de la conversación o el vendedor ocupó ese rol hablando de sus propios logros? El cliente siempre debe ser el protagonista.

**Desapego:**
- ¿El vendedor transmitió genuinamente que no está ahí para vender a cualquier precio? ¿Comunicó que solo trabaja con quien realmente puede ayudar y que lo último que quiere es que el cliente gaste en algo que no le sirva? El desapego genera más confianza y más ventas.

**Uso de metáforas y analogías:**
- ¿Explicó conceptos complejos usando historias o analogías simples y familiares para ese cliente en particular? ("Esto es como cuando estabas aprendiendo a caminar, porque...")

**Asentimiento confirmatorio:**
- ¿Asintió con la cabeza (o con expresiones como "sí", "claro", "entiendo") mientras el cliente explicaba su problema? Este gesto programa al cerebro del cliente para decir "sí" más adelante.

---

### 17. ESCUCHA ACTIVA
La escucha activa es prestar atención real al lead, comprender lo que dice y usar esa información para profundizar y orientar la conversación respondiendo en base a lo que el lead expresa.

**Lo que debe hacer el vendedor (ejemplos positivos):**
- Cuando el lead menciona algo personal o emotivo ("vi la clase a medias porque mi hijo está enfermo"), el vendedor lo registra y responde a eso antes de continuar con la venta.
- Cuando el lead dice algo relevante sobre su situación, el vendedor lo toma y pregunta para profundizar: "Qué fundamental que hayas podido detectar tus errores. ¿Podrías comentarme cuáles son?"
- Cuando el lead dice "me gustaría hacerlo, pero…", el vendedor no atropella: "Nadie nos corre, comentame cuál es la duda que te quedó."

**Señales de escucha activa AUSENTE (anti-ejemplos):**
- El lead dice algo relevante y el vendedor ignora completamente ese punto y cambia de tema.
- El lead menciona una situación personal (hijo enfermo, problema personal) y el vendedor continúa con el guion sin acusarlo.
- El lead empieza a expresar una duda o un "pero…" y el vendedor lo interrumpe yendo directo al cierre o al precio.

**Preguntas para evaluar:**
- ¿El vendedor respondió en base a lo que el lead acababa de decir, o siguió un guion independientemente de la respuesta?
- ¿El vendedor recogió las palabras exactas del lead para hacer la siguiente pregunta?
- ¿El lead se sintió escuchado y comprendido, o fue procesado como un número más?

---

### 18. CREATIVIDAD EN LA COMUNICACIÓN
La creatividad es la capacidad de comunicar de forma diferente, salir del esquema tradicional y generar impacto emocional en el lead usando metáforas, analogías y comparaciones originales.

**Lo que debe hacer el vendedor (ejemplos positivos):**
- Usar metáforas que conecten el producto con algo familiar para ese lead específico: "Es como un GPS: no camina por vos, pero te muestra el camino más corto para llegar a donde querés."
- Usar analogías visuales: "Si tu emprendimiento fuera una casa, hoy estaríamos ordenando los cimientos antes de seguir construyendo."
- Hacer que el lead visualice su situación: "Hoy estás intentando armar un rompecabezas sin la imagen de referencia. Nosotros te damos esa imagen."

**Señales de creatividad AUSENTE (anti-ejemplos):**
- Describir el producto solo con características técnicas: "Tiene 12 semanas, 2 clases por semana y material grabado."
- Usar frases genéricas que no conectan con el lead: "Es un programa online con módulos y mentorías."
- Hablar del acceso y la plataforma sin conectarlo con la transformación del cliente.

**Preguntas para evaluar:**
- ¿El vendedor usó alguna metáfora, analogía o imagen mental para explicar el valor del producto?
- ¿Las comparaciones que usó conectaban con el mundo o la realidad específica de ese lead?
- ¿El lead pudo "ver" o "sentir" la solución, o solo recibió información técnica?
""".strip()

FEEDBACK_CRITERIA = os.getenv("FEEDBACK_CRITERIA", _DEFAULT_CRITERIA)


def get_missing_configs():
    missing = []
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    return missing
