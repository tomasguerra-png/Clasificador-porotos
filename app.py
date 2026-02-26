import csv
import io
import re
import time
from datetime import datetime

import streamlit as st
import pandas as pd

from classifier import PorotoclassifierLLM, OUTPUT_FIELDS, GROQ_MODELS
from jira_client import JiraClient

st.set_page_config(
    page_title="Clasificador de Porotos TMO",
    page_icon="🫘",
    layout="wide",
)

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def get_current_quarter():
    now = datetime.now()
    q = (now.month - 1) // 3 + 1
    return f"Q{q}-{now.year}"


def extract_key(text, prefix="SMPR"):
    m = re.search(rf"({prefix}-\d+)", str(text))
    return m.group(1) if m else None


def parse_uploaded_csv(uploaded_file):
    content = uploaded_file.getvalue().decode("utf-8-sig")
    sep = ";"
    for s in [";", ",", "\t"]:
        if s in content[:2000]:
            sep = s
            break

    reader = csv.reader(io.StringIO(content), delimiter=sep)
    rows = list(reader)
    if not rows:
        return []

    has_header = not any("SMPR-" in cell for cell in rows[0])
    start = 1 if has_header else 0

    porotos = []
    for row in rows[start:]:
        if not row or not any(cell.strip() for cell in row):
            continue
        key = None
        title = ""
        key_idx = -1
        for i, cell in enumerate(row):
            found = extract_key(cell.strip(), "SMPR")
            if found:
                key = found
                key_idx = i
                break
        if key:
            for i, cell in enumerate(row):
                val = cell.strip()
                if i != key_idx and val and not val.startswith("http") and "SMPR-" not in val and len(val) > 5:
                    title = val
                    break
            porotos.append({"key": key, "title": title})
    return porotos


def results_to_dataframe(results):
    records = []
    for r in results:
        records.append({
            "clave": r.get("key", ""),
            "resumen": r.get("title", ""),
            "ANTIGUEDAD": r.get("ANTIGUEDAD", ""),
            "TIPO_DE_PRODUCTO": r.get("TIPO_DE_PRODUCTO", ""),
            "SCOPE": r.get("SCOPE", ""),
            "COMPLEJIDAD": r.get("COMPLEJIDAD", ""),
            "SCOPE_REFINAMIENTO": r.get("SCOPE_REFINAMIENTO", ""),
            "JUSTIFICACION": r.get("JUSTIFICACION", ""),
        })
    return pd.DataFrame(records)


def df_to_csv_bytes(df):
    return df.to_csv(index=False, sep=";").encode("utf-8")


def color_antiguedad(val):
    colors = {
        "Nuevo": "background-color: #d4edda; color: #155724",
        "Carry Over": "background-color: #fff3cd; color: #856404",
        "N/A": "background-color: #f8d7da; color: #721c24",
        "ERROR": "background-color: #f5c6cb; color: #721c24",
    }
    return colors.get(val, "")


# ──────────────────────────────────────────────
# Credentials
# ──────────────────────────────────────────────

def get_credentials():
    creds = {"groq_key": "", "jira_url": "https://mercadolibre.atlassian.net", "jira_email": "", "jira_token": ""}
    try:
        creds["groq_key"] = st.secrets.get("groq", {}).get("api_key", "")
        creds["jira_url"] = st.secrets.get("jira", {}).get("base_url", creds["jira_url"])
        creds["jira_email"] = st.secrets.get("jira", {}).get("email", "")
        creds["jira_token"] = st.secrets.get("jira", {}).get("api_token", "")
    except Exception:
        pass
    return creds


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────

def render_sidebar():
    creds = get_credentials()
    with st.sidebar:
        st.header("Configuracion")

        speed = st.radio(
            "Velocidad del modelo",
            options=["⚡ Rápido (~2s/poroto)", "🎯 Preciso (~4s/poroto)"],
            index=0,
            help="Rápido usa un modelo más chico pero mucho más veloz. Preciso usa el modelo grande.",
        )
        creds["model_speed"] = "fast" if "Rápido" in speed else "accurate"

        with st.expander("API Keys", expanded=not creds["groq_key"]):
            groq_key = st.text_input("Groq API Key", value=creds["groq_key"], type="password",
                                     help="Gratis en https://console.groq.com/keys")
            jira_email = st.text_input("Jira Email", value=creds["jira_email"])
            jira_token = st.text_input("Jira API Token", value=creds["jira_token"], type="password",
                                       help="https://id.atlassian.com/manage-profile/security/api-tokens")
            creds["groq_key"] = groq_key
            creds["jira_email"] = jira_email
            creds["jira_token"] = jira_token

        st.divider()
        st.subheader("Estado")
        model_name = GROQ_MODELS.get(creds["model_speed"], "llama-3.1-8b-instant")
        if creds["groq_key"]:
            st.success(f"LLM: {model_name}", icon="✅")
        else:
            st.error("Falta API key de LLM", icon="❌")
        if creds["jira_email"] and creds["jira_token"]:
            st.success("Jira conectado", icon="✅")
        else:
            st.warning("Sin Jira (solo titulo)", icon="⚠️")

        st.divider()
        st.caption("Campos generados:")
        st.code("ANTIGUEDAD\nTIPO_DE_PRODUCTO\nSCOPE\nCOMPLEJIDAD\nSCOPE_REFINAMIENTO\nJUSTIFICACION", language=None)

    return creds


# ──────────────────────────────────────────────
# Classification
# ──────────────────────────────────────────────

def run_classification(porotos, creds):
    if not creds.get("groq_key"):
        st.error("Configura la API key de Groq en el sidebar.")
        return None

    model = GROQ_MODELS.get(creds.get("model_speed", "fast"), "llama-3.1-8b-instant")
    classifier = PorotoclassifierLLM(provider="groq", api_key=creds["groq_key"], model=model)
    st.caption(f"Modelo: **{classifier.provider_name}**")

    jira = None
    jira_ok = False
    if creds.get("jira_email") and creds.get("jira_token"):
        jira = JiraClient(creds["jira_url"], creds["jira_email"], creds["jira_token"])
        try:
            test = jira.get_issue(porotos[0]["key"])
            if test:
                jira_ok = True
                st.success(f"Jira conectado OK - probado con {porotos[0]['key']}")
            else:
                st.warning(f"Jira no encontró {porotos[0]['key']}. Verificá credenciales.")
                jira = None
        except Exception as e:
            st.warning(f"Jira no disponible ({e}). Usando títulos del CSV.")
            jira = None
    else:
        has_titles = any(p.get("title") for p in porotos)
        if has_titles:
            st.info("Sin Jira. Usando títulos del CSV para clasificar.")
        else:
            st.error("Sin Jira y el CSV no tiene títulos. Agregá una columna con los títulos de los porotos.")

    results = []
    total = len(porotos)
    jira_errors = 0
    progress_bar = st.progress(0, text=f"Clasificando 0/{total}...")
    status_text = st.empty()
    results_container = st.empty()
    start_time = time.time()

    for i, poroto in enumerate(porotos):
        key = poroto["key"]
        title = poroto.get("title", "")
        description = ""
        labels = []
        components = []

        if jira:
            try:
                details = jira.get_issue_details(key)
                if details:
                    title = details["title"]
                    description = details["description"]
                    labels = details["labels"]
                    components = details["components"]
                else:
                    jira_errors += 1
            except Exception:
                jira_errors += 1

        if not title:
            row = {"key": key, "title": ""}
            for field in OUTPUT_FIELDS:
                row[field] = ""
            row["ANTIGUEDAD"] = "ERROR"
            row["JUSTIFICACION"] = "No se pudo obtener info del ticket (sin Jira ni titulo en CSV)"
            results.append(row)
        else:
            result = classifier.classify(key, title, description, labels, components)
            row = {"key": key, "title": title}
            for field in OUTPUT_FIELDS:
                row[field] = result.get(field, "")
            results.append(row)

        done = i + 1
        elapsed = time.time() - start_time
        avg = elapsed / done
        remaining_min = avg * (total - done) / 60

        progress_bar.progress(done / total, text=f"Clasificando {done}/{total}...")
        status_text.caption(f"⏱️ {avg:.1f}s/poroto  |  ~{remaining_min:.0f} min restantes  |  Último: {key}")

        if done % 5 == 0 or done == total:
            df_partial = results_to_dataframe(results)
            results_container.dataframe(
                df_partial,
                use_container_width=True,
                height=min(250, 35 * len(df_partial) + 38),
            )

    progress_bar.progress(1.0, text=f"✅ Listo: {total}/{total} clasificados en {elapsed:.0f}s")
    status_text.empty()
    if jira_errors > 0:
        st.warning(f"⚠️ {jira_errors} tickets no se pudieron leer de Jira. Verificá las credenciales en el sidebar.")
    return results


# ──────────────────────────────────────────────
# Results display
# ──────────────────────────────────────────────

def show_results(df):
    st.divider()
    st.subheader("Resultados")

    total = len(df)
    if total > 0:
        c1, c2, c3, c4 = st.columns(4)
        nuevo = len(df[df["ANTIGUEDAD"] == "Nuevo"])
        carry = len(df[df["ANTIGUEDAD"] == "Carry Over"])
        na = len(df[df["ANTIGUEDAD"] == "N/A"])
        errors = len(df[df["ANTIGUEDAD"] == "ERROR"])
        c1.metric("Nuevo", nuevo, f"{nuevo/total*100:.0f}%")
        c2.metric("Carry Over", carry, f"{carry/total*100:.0f}%")
        c3.metric("N/A", na, f"{na/total*100:.0f}%")
        if errors:
            c4.metric("Errores", errors, "⚠️")

    st.dataframe(
        df.style.map(color_antiguedad, subset=["ANTIGUEDAD"]),
        use_container_width=True,
        height=500,
    )

    csv_bytes = df_to_csv_bytes(df)
    st.download_button(
        label="📥 Descargar CSV clasificado",
        data=csv_bytes,
        file_name=f"Resultado_Clasificado_{get_current_quarter()}.csv",
        mime="text/csv",
        type="primary",
        use_container_width=True,
    )


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    creds = render_sidebar()

    st.title("🫘 Clasificador de Porotos TMO")
    st.caption("Subí el CSV con los porotos del quarter. El clasificador lee cada ticket de Jira y lo clasifica automáticamente.")

    uploaded = st.file_uploader("Subí tu CSV de porotos", type=["csv"],
                                help="CSV con al menos una columna con IDs SMPR-XXXXX")

    if uploaded is None:
        st.info("Esperando un archivo CSV con los porotos del quarter...")
        return

    porotos = parse_uploaded_csv(uploaded)
    if not porotos:
        st.error("No se encontraron IDs de porotos (SMPR-XXXXX) en el archivo.")
        return

    titles_count = sum(1 for p in porotos if p.get("title"))
    st.success(f"Se encontraron **{len(porotos)}** porotos en el archivo ({titles_count} con título).")

    col1, col2 = st.columns([1, 2])
    with col1:
        classify_btn = st.button("🚀 Clasificar", type="primary", use_container_width=True)
    with col2:
        speed = creds.get("model_speed", "fast")
        secs = 2 if speed == "fast" else 4
        est_min = len(porotos) * secs / 60
        st.caption(f"Tiempo estimado: ~{est_min:.0f} min ({secs}s/poroto en modo {'rápido' if speed == 'fast' else 'preciso'})")

    if "results_df" in st.session_state and not classify_btn:
        show_results(st.session_state["results_df"])
        return

    if not classify_btn:
        return

    if not creds.get("groq_key"):
        st.error("Configura la API key de Groq en el sidebar antes de clasificar.")
        return

    results = run_classification(porotos, creds)
    if results:
        df = results_to_dataframe(results)
        st.session_state["results_df"] = df
        show_results(df)


if __name__ == "__main__":
    main()
