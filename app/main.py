from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException

from app.config import DATABASES
from app.llm.planner import generate_ir
from app.graph.schema_graph import build_schema_graph
from app.planner.resolver import resolve_query
from app.compiler.mysql import compile_sql
from app.db.executor import execute_sql

app = FastAPI()
schema_cache = {}


@app.post("/query")
def query_db(payload: dict):
    try:
        db_name = payload["database"]
        user_query = payload["query"]
    except KeyError:
        raise HTTPException(status_code=400, detail="Payload must contain database and query")

    if db_name not in DATABASES:
        raise HTTPException(status_code=400, detail="Unknown database")

    db = DATABASES[db_name]
    password = quote_plus(db["password"])

    db_url = (
        f"mysql+pymysql://{db['user']}:{password}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )

    if db_name not in schema_cache:
        schema_cache[db_name] = build_schema_graph(db_url)

    ir = generate_ir(user_query)
    plan = resolve_query(ir, schema_cache[db_name])
    sql = compile_sql(plan)
    data = execute_sql(db_url, sql)

    return {
        "ir": ir,
        "sql": sql,
        "data": data
    }
