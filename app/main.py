from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException

from app.config import DATABASES
from app.graph.schema_graph import build_schema_context
from app.llm.planner import generate_sql
from app.db.executor import execute_sql

app = FastAPI()
schema_cache = {}


@app.post("/query")
def query_db(payload: dict):
    if "database" not in payload or "query" not in payload:
        raise HTTPException(
            status_code=400,
            detail="Payload must contain 'database' and 'query'"
        )

    db_name = payload["database"]
    user_query = payload["query"]

    if db_name not in DATABASES:
        raise HTTPException(status_code=400, detail="Unknown database")

    db = DATABASES[db_name]
    password = quote_plus(db["password"])

    db_url = (
        f"mysql+pymysql://{db['user']}:{password}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )

    if db_name not in schema_cache:
        schema_cache[db_name] = build_schema_context(db_url)

    schema_context = schema_cache[db_name]

    sql = generate_sql(user_query, schema_context)

    data = execute_sql(db_url, sql)

    return {
        "sql": sql,
        "data": data
    }
