from app.llm.planner import generate_sql

def repair_sql(
    invalid_sql: str,
    error_message: str,
    schema: dict,
    user_query: str
) -> str:
    """
    Attempts to repair invalid SQL using the LLM,
    grounded by schema and original user intent.
    """
    repair_prompt = f"""
The following SQL query is INVALID.

ORIGINAL QUESTION:
{user_query}

INVALID SQL:
{invalid_sql}

VALIDATION ERROR:
{error_message}

TASK:
- Fix the SQL while preserving the original intent
- Use ONLY the provided schema and foreign keys
- Do NOT introduce new tables or columns
- Return ONLY a valid MySQL SELECT query
"""
    return generate_sql(repair_prompt, schema)
