import json
import re
import requests

from app.config import OLLAMA_URL, OLLAMA_MODEL


def generate_ir(user_query: str) -> dict:
    prompt = f"""
You convert natural language questions into a semantic JSON query intent.

RULES:
- Output ONLY valid JSON
- No explanations
- No markdown
- No backticks

JSON SCHEMA:
{{
  "entity": "Customer | Film | Actor | Rental",
  "metric": "Revenue | Count | List",
  "time_range": "last_month | all_time | null",
  "limit": number
}}

Question:
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

    raw = re.sub(r"^```json|```$", "", raw, flags=re.MULTILINE).strip()

    try:
        ir = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from LLM:\n{raw}") from e

    # Defensive defaults
    ir.setdefault("time_range", None)
    ir.setdefault("limit", 10)

    return ir
