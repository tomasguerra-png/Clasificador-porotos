import json
import time
import os
import requests as http_requests

SYSTEM_PROMPT = """\
Sos un clasificador de "porotos" (tickets Jira trimestrales) para TMO \
(Transaction Management & Operations) de Mercado Libre / Mercado Pago.

TMO: conciliación de transacciones, integración con bancos/procesadores, \
control de costos, liquidaciones. Herramientas: Simetrik, Modena, OneConci, Recon API.

Clasificá en 6 campos:

### 1. ANTIGUEDAD: "Nuevo" | "Carry Over" | "N/A"
- "Carry Over": titulo dice [Carry Over], Carryover, continuación de Q anterior.
- "N/A": NO impacta conciliación TMO (compensación comisiones, UX, pricing, Ledger sin conci, infra no-TMO).
- "Nuevo": todo lo demás.

### 2. TIPO_DE_PRODUCTO (solo si Nuevo, sino ""):
- "Nueva Conexion": partner/banco/procesador COMPLETAMENTE nuevo con el que NUNCA se integró antes. [CD] (conexión directa), nuevo sponsor bank, nuevo adquirente. NO usar si es feature nuevo sobre conexión existente (ej: DCC sobre Génova = Mejora, upgrade de versión = Mejora).
- "Nuevo Producto": producto financiero COMPLETAMENTE nuevo (BNPL, licencia bancaria, fondo inversión, bimonetarismo, cuenta remunerada).
- "Mejora o modificacion de conexion existente": DEFAULT, el más común. Cualquier cambio/feature/mejora sobre flujos existentes: rollouts, control costos, cambios regulatorios, tokenización, MSI, DCC, nuevas features sobre procesadores ya integrados, upgrades de versión, reingeniería.

### 3. SCOPE (solo si Nuevo, sino ""):
- "Soporte": TMO solo acompaña/asiste. [Scope: Rollout], [RollOut], pruebas, monitoreo, config menor, rollout de tokenización, adaptar soluciones existentes.
- "Analisis": TMO SOLO analiza impacto SIN implementar. Título dice [Scope: Analisis]. MUY RARO, casi no se usa.
- "Desarrollo": DEFAULT y MÁS COMÚN. TMO implementa/desarrolla algo. [Scope: Desarrollo], ETLs, APIs, controles, implementaciones, integraciones, conexiones nuevas, TCH, tokenización, mejoras de flujos.
- "Analisis y Desarrollo": SOLO cuando el título dice EXPLÍCITAMENTE [A&D] o la descripción menciona una fase de análisis seguida de implementación con alta incertidumbre. Es POCO común. Si dudás entre Desarrollo y Analisis y Desarrollo, elegí Desarrollo.

### 4. COMPLEJIDAD (solo si Nuevo, sino ""):
- "Poroto abarca mas de un flujo": SI el título o descripción menciona 2+ sites (MLA, MLB, MLM, MLC, MCO, MLU, MEC, MPE) o dice "all sites", "cross-site", "multi-site", "todos los sites". También si menciona múltiples flujos/conexiones distintas.
- "Poroto abarca solo un flujo": DEFAULT. Solo 1 site o 1 flujo, o no se mencionan sites.
CLAVE: Si ves "[MLM, MLB, MLA]" o "in all sites" o "Point in all sites" → SIEMPRE es "mas de un flujo".

### 5. SCOPE_REFINAMIENTO: mismo valor que SCOPE (repetir).

### 6. JUSTIFICACION: 1 oración en español explicando.

## EJEMPLOS
"[Carry Over] Banorte - Dictamen técnico" → Carry Over, campos 2-5 vacíos.
"AMEX - Compensación comisiones" → N/A, campos 2-5 vacíos.
"Diseño experiencia Flow APIs" → N/A, no TMO.
"Reingenieria conciliacion Monza MLB" → Nuevo, Mejora, Desarrollo, solo un flujo.
"MLA 2nd sponsor Bank Brubank Tip IN" → Nuevo, Nueva Conexion, Desarrollo, solo un flujo.
"P_784 Producto virtual Telemedicina" → Nuevo, Nuevo Producto, Desarrollo, solo un flujo.
"Benefits Orquestador" → Nuevo, Nuevo Producto, Desarrollo, solo un flujo.
"Implementación control pagos cuotas MLC" → Nuevo, Mejora, Soporte, solo un flujo.
"[Conexiones Directas MLM] TELCEL Postpago" → Carry Over (viene de Q anterior).
"[MLM, MLB, MLA] Mastercard Solución reapertura escenarios" → Nuevo, Mejora, Desarrollo, mas de un flujo.
"Conexion via API - BBVA Mexico" → Nuevo, Mejora (API sobre banco ya integrado), Desarrollo, solo un flujo.
"[RollOut] Conexión a versión web 2.0 Izipay" → Nuevo, Mejora, Soporte, solo un flujo.
"[Scope: Rollout] MLM Acquirer BBVA - Conciliación Pagos Tokenizados" → Nuevo, Mejora, Soporte, solo un flujo.
"TCH en MCO para tener Tokenizacion" → Nuevo, Mejora, Desarrollo, solo un flujo.
"[MLU] Conexión GetNet - Lanzamiento Smart N950" → Nuevo, Nueva Conexion, Desarrollo, solo un flujo.
"Actualización de la conexión Credibanco" → Nuevo, Mejora, Desarrollo, solo un flujo.
"Puntos BBVA - Incorporar como medio de pago en ON" → Nuevo, Nueva Conexion, Desarrollo, solo un flujo.
"Estandarización de Devoluciones Parciales en Mercado Pago" → Nuevo, Mejora, Soporte, solo un flujo.
"BBVA Interredes - Tokenización (Card on file)" → Nuevo, Mejora, Soporte, solo un flujo.
"[MLC] [A&D] - Promos Bancarias MLC ON/OFF" → Nuevo, Mejora, Analisis y Desarrollo, solo un flujo (A&D explícito en título).

## REGLAS ESTRICTAS
- Carry Over/N/A → TIPO_DE_PRODUCTO, SCOPE, COMPLEJIDAD, SCOPE_REFINAMIENTO = "".
- Nuevo → TIPO_DE_PRODUCTO, SCOPE, COMPLEJIDAD son OBLIGATORIOS, NUNCA vacíos.
  Si no estás seguro, usá los defaults: TIPO_DE_PRODUCTO="Mejora o modificacion de conexion existente", SCOPE="Desarrollo", COMPLEJIDAD="Poroto abarca solo un flujo".
- SCOPE_REFINAMIENTO = SCOPE siempre.

Respondé SOLO JSON válido (sin markdown):
{"ANTIGUEDAD":"...","TIPO_DE_PRODUCTO":"...","SCOPE":"...","COMPLEJIDAD":"...","SCOPE_REFINAMIENTO":"...","JUSTIFICACION":"..."}
"""

OUTPUT_FIELDS = [
    "ANTIGUEDAD", "TIPO_DE_PRODUCTO", "SCOPE", "COMPLEJIDAD",
    "SCOPE_REFINAMIENTO", "JUSTIFICACION",
]

_TIPO_PRODUCTO_NORM = {
    "mejora": "Mejora o modificacion de conexion existente",
    "mejora existente": "Mejora o modificacion de conexion existente",
    "mejora o modificacion": "Mejora o modificacion de conexion existente",
    "mejora o modificación de conexión existente": "Mejora o modificacion de conexion existente",
    "nueva conexion": "Nueva Conexion",
    "nueva conexión": "Nueva Conexion",
    "nuevo producto": "Nuevo Producto",
}

_SCOPE_NORM = {
    "analisis": "Analisis",
    "análisis": "Analisis",
    "desarrollo": "Desarrollo",
    "soporte": "Soporte",
    "analisis y desarrollo": "Analisis y Desarrollo",
    "análisis y desarrollo": "Analisis y Desarrollo",
    "a&d": "Analisis y Desarrollo",
}

_ANTIGUEDAD_NORM = {
    "nuevo": "Nuevo",
    "carry over": "Carry Over",
    "carryover": "Carry Over",
    "n/a": "N/A",
    "na": "N/A",
}


def _normalize(result):
    ant = result.get("ANTIGUEDAD", "")
    result["ANTIGUEDAD"] = _ANTIGUEDAD_NORM.get(ant.lower().strip(), ant)

    if result["ANTIGUEDAD"] in ("Carry Over", "N/A"):
        result["TIPO_DE_PRODUCTO"] = ""
        result["SCOPE"] = ""
        result["COMPLEJIDAD"] = ""
        result["SCOPE_REFINAMIENTO"] = ""
        return result

    tp = result.get("TIPO_DE_PRODUCTO", "")
    result["TIPO_DE_PRODUCTO"] = _TIPO_PRODUCTO_NORM.get(tp.lower().strip(), tp)
    if not result["TIPO_DE_PRODUCTO"]:
        result["TIPO_DE_PRODUCTO"] = "Mejora o modificacion de conexion existente"

    sc = result.get("SCOPE", "")
    result["SCOPE"] = _SCOPE_NORM.get(sc.lower().strip(), sc)
    if not result["SCOPE"]:
        result["SCOPE"] = "Desarrollo"

    comp = result.get("COMPLEJIDAD", "")
    if not comp:
        result["COMPLEJIDAD"] = "Poroto abarca solo un flujo"

    result["SCOPE_REFINAMIENTO"] = result["SCOPE"]
    return result


GROQ_MODELS = {
    "fast": "llama-3.1-8b-instant",
    "accurate": "llama-3.3-70b-versatile",
}


class LLMProvider:
    def __init__(self, provider, api_key, model=None):
        self.provider = provider.lower()
        self.api_key = api_key
        if self.provider == "groq":
            self.base_url = "https://api.groq.com/openai/v1/chat/completions"
            self.model = model or "llama-3.1-8b-instant"
            self.min_interval = 0.5
        elif self.provider == "gemini":
            self.base_url = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            self.model = model or "gemini-2.0-flash"
            self.min_interval = 4.2
        elif self.provider == "openai":
            self.base_url = "https://api.openai.com/v1/chat/completions"
            self.model = model or "gpt-4o-mini"
            self.min_interval = 0.5
        else:
            raise ValueError(f"Provider '{provider}' no soportado.")

    def call(self, system_prompt, user_message):
        if self.provider == "gemini":
            return self._call_gemini(system_prompt, user_message)
        return self._call_openai_compat(system_prompt, user_message)

    def _call_openai_compat(self, system_prompt, user_message):
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.1,
            "max_tokens": 300,
            "response_format": {"type": "json_object"},
        }
        resp = http_requests.post(self.base_url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 429:
            retry_after = resp.headers.get("retry-after")
            wait = float(retry_after) if retry_after else 3.0
            raise RateLimitError(wait)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    def _call_gemini(self, system_prompt, user_message):
        url = self.base_url.format(model=self.model) + f"?key={self.api_key}"
        payload = {
            "system_instruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_message}]}],
            "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"},
        }
        resp = http_requests.post(url, json=payload, timeout=30)
        if resp.status_code == 429:
            raise RateLimitError(5.0)
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]


class RateLimitError(Exception):
    def __init__(self, wait_seconds):
        self.wait_seconds = wait_seconds
        super().__init__(f"Rate limited, wait {wait_seconds}s")


def _detect_provider():
    groq_key = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")
    if groq_key:
        return "groq", groq_key
    if gemini_key:
        return "gemini", gemini_key
    if openai_key:
        return "openai", openai_key
    return None, None


class PorotoclassifierLLM:
    def __init__(self, provider=None, api_key=None, model=None):
        if provider is None or api_key is None:
            provider, api_key = _detect_provider()
        if not provider or not api_key:
            raise RuntimeError(
                "No se encontro API key de LLM. Configura al menos una:\n"
                "  GROQ_API_KEY (gratis en https://console.groq.com/keys)"
            )
        self.llm = LLMProvider(provider, api_key, model)
        self.last_request_time = 0

    @property
    def provider_name(self):
        return f"{self.llm.provider} / {self.llm.model}"

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.llm.min_interval:
            time.sleep(self.llm.min_interval - elapsed)
        self.last_request_time = time.time()

    def classify(self, ticket_key, title, description="", labels=None, components=None, max_retries=5):
        user_msg = f"Ticket: {ticket_key}\nTítulo: {title}\n"
        if description:
            user_msg += f"\nDescripción:\n{description[:2000]}\n"
        if labels:
            user_msg += f"\nLabels: {', '.join(labels)}\n"
        if components:
            user_msg += f"\nComponents: {', '.join(components)}\n"

        for attempt in range(max_retries):
            try:
                self._rate_limit()
                raw = self.llm.call(SYSTEM_PROMPT, user_msg)
                text = raw.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

                result = json.loads(text)
                if "ANTIGUEDAD" not in result:
                    raise ValueError("Missing ANTIGUEDAD")

                for field in OUTPUT_FIELDS:
                    result.setdefault(field, "")

                result = _normalize(result)
                return result

            except RateLimitError as e:
                wait = min(e.wait_seconds + (attempt * 2), 20)
                time.sleep(wait)
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return {f: "" for f in OUTPUT_FIELDS} | {
                        "ANTIGUEDAD": "ERROR",
                        "JUSTIFICACION": f"Error: {e}",
                    }

        return {f: "" for f in OUTPUT_FIELDS} | {
            "ANTIGUEDAD": "ERROR",
            "JUSTIFICACION": "Error: rate limit agotado tras reintentos",
        }
