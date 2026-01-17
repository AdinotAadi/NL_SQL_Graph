import json
import re
import requests

from app.config import OLLAMA_URL, OLLAMA_MODEL


def generate_sql(user_query: str, schema_context: dict) -> str:
    tables = schema_context["tables"]
    graph = schema_context["graph"]

    schema_description = []
    for table, cols in tables.items():
        schema_description.append(f"{table}({', '.join(cols)})")

    fk_description = []
    for u, v, data in graph.edges(data=True):
        left, right = data["join_on"]
        fk_description.append(f"{left} = {right}")

    prompt = f"""
You are an expert SQL generator.

DATABASE SCHEMA:
{chr(10).join(schema_description)}

FOREIGN KEY JOINS:
{chr(10).join(fk_description)}

RULES:
- Generate ONLY a single valid MySQL SELECT statement
- Use correct joins based on foreign keys
- Do NOT hallucinate tables or columns
- Do NOT use UPDATE, DELETE, INSERT
- No markdown, no explanations

QUESTION:
{user_query}
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False
        },
        timeout=60
    )

    response.raise_for_status()

    raw = response.json().get("response", "").strip()
    raw = re.sub(r"^```sql|```$", "", raw, flags=re.MULTILINE).strip()

    if not raw.lower().startswith("select"):
        raise ValueError("LLM did not return a SELECT statement")

    return raw
