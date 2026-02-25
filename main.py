"""
Clasificador automático de Porotos TMO (CLI).

Uso:
    python main.py <input.csv> [output.csv]
"""

import csv
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

from classifier import PorotoclassifierLLM, OUTPUT_FIELDS
from jira_client import JiraClient


def extract_ticket_key(text):
    m = re.search(r"(SMPR-\d+)", text)
    return m.group(1) if m else None


def read_input_csv(filepath):
    with open(filepath, "r", encoding="utf-8-sig") as f:
        sample = f.read(4096)
    sep = ";"
    for s in [";", ",", "\t"]:
        if s in sample:
            sep = s
            break

    porotos = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=sep)
        rows = list(reader)

    has_header = not any("SMPR-" in c for c in rows[0]) if rows else False
    start = 1 if has_header else 0

    for row in rows[start:]:
        if not row:
            continue
        key = None
        for cell in row:
            key = extract_ticket_key(cell.strip())
            if key:
                break
        if key:
            porotos.append({"key": key})
    return porotos


def save_results(results, path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["clave", "resumen"] + OUTPUT_FIELDS)
        for r in results:
            writer.writerow([
                r.get("key", ""),
                r.get("title", ""),
                *[r.get(f, "") for f in OUTPUT_FIELDS],
            ])


def main():
    script_dir = Path(__file__).resolve().parent
    load_dotenv(script_dir / ".env")

    if len(sys.argv) < 2:
        print("Uso: python main.py <input.csv> [output.csv]")
        sys.exit(1)

    input_path = sys.argv[1]
    default_out = str(Path.home() / "Desktop" / "RESULTADO_CLASIFICADO.csv")
    output_path = sys.argv[2] if len(sys.argv) > 2 else default_out

    if not os.path.exists(input_path):
        print(f"Error: {input_path} no encontrado")
        sys.exit(1)

    try:
        classifier = PorotoclassifierLLM()
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)
    print(f"[OK] LLM: {classifier.provider_name}")

    jira_url = os.getenv("JIRA_BASE_URL")
    jira_email = os.getenv("JIRA_EMAIL")
    jira_token = os.getenv("JIRA_API_TOKEN")
    jira = None
    if jira_email and jira_token and jira_url:
        jira = JiraClient(jira_url, jira_email, jira_token)
        print(f"[OK] Jira: {jira_url}")
    else:
        print("[!!] Sin Jira, clasificando solo por titulo")

    porotos = read_input_csv(input_path)
    print(f"Encontrados: {len(porotos)} porotos\n")

    results = []
    for poroto in tqdm(porotos, desc="Clasificando", unit="poroto"):
        key = poroto["key"]
        title = ""
        desc = ""
        labels = []
        components = []

        if jira:
            try:
                d = jira.get_issue_details(key)
                if d:
                    title = d["title"]
                    desc = d["description"]
                    labels = d["labels"]
                    components = d["components"]
            except Exception as e:
                tqdm.write(f"  [!] Jira {key}: {e}")

        result = classifier.classify(key, title, desc, labels, components)
        row = {"key": key, "title": title}
        for f in OUTPUT_FIELDS:
            row[f] = result.get(f, "")
        results.append(row)

    save_results(results, output_path)
    print(f"\nResultado guardado en: {output_path}")

    counts = {}
    for r in results:
        a = r.get("ANTIGUEDAD", "?")
        counts[a] = counts.get(a, 0) + 1
    print("\nResumen:")
    for k, v in sorted(counts.items()):
        print(f"  {k}: {v} ({v/len(results)*100:.0f}%)")


if __name__ == "__main__":
    main()
