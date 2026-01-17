# app/llm/planner.py
import re
import requests
from app.config import OLLAMA_URL, OLLAMA_MODEL

def generate_sql(user_query: str, schema: dict) -> str:
    """
    Generate a SQL query using the LLM, strictly grounded in the schema.
    Prevents unnecessary joins and hallucinatory columns.
    """

    tables = schema["tables"]
    graph = schema["graph"]

    # Build schema description for the LLM
    table_desc = [f"{t}({', '.join(cols)})" for t, cols in tables.items()]
    join_desc = [f"{a} = {b}" for _, _, d in graph.edges(data=True) for a, b in [d["join_on"]]]

    prompt = f"""
You are an expert MySQL query generator.

DATABASE TABLES:
{chr(10).join(table_desc)}

VALID FOREIGN KEY JOINS:
{chr(10).join(join_desc)}

RULES:
- Generate ONE valid MySQL SELECT query.
- Use only the listed tables and columns.
- Use joins ONLY if necessary to answer the question.
- Never introduce unrelated tables or columns.
- Always check that columns exist in the schema.
- Use table aliases (T1, T2, etc.) consistently.
- Do NOT use INSERT, UPDATE, DELETE, DROP, ALTER.
- Return only a single SQL query.
- No markdown, no explanations.

QUESTION:
{user_query}
"""

    res = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False
        },
        timeout=60
    )

    res.raise_for_status()
    sql = res.json()["response"].strip()
    # Remove markdown code blocks if present
    sql = re.sub(r"^```sql|```$", "", sql, flags=re.MULTILINE).strip()

    if not sql.lower().startswith("select"):
        raise ValueError("LLM did not return a SELECT query")

    return sql
