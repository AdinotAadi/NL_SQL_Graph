from fastapi import FastAPI, HTTPException
from urllib.parse import quote_plus

from app.graph.schema_graph import build_schema_context
from app.llm.planner import generate_sql
from app.compiler.sql_validator import validate_sql
from app.compiler.sql_repair import repair_sql
from app.db.executor import execute_sql
from app.config import DATABASES

app = FastAPI()
schema_cache = {}


@app.post("/query")
def query_db(payload: dict):
    user_query = payload.get("query")
    if not user_query:
        raise HTTPException(status_code=400, detail="Query not provided")

    db_name = payload.get("database", "sakila")
    if db_name not in DATABASES:
        raise HTTPException(status_code=400, detail=f"Unknown database: {db_name}")

    # --- Build schema cache if not present ---
    if db_name not in schema_cache:
        db_config = DATABASES[db_name]
        password = quote_plus(db_config["password"])
        db_url = f"mysql+pymysql://{db_config['user']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        try:
            schema_cache[db_name] = build_schema_context(db_url)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to connect to database: {str(e)}")

    schema = schema_cache[db_name]

    try:
        # Generate SQL using LLM
        sql = generate_sql(user_query, schema)
        # Validate generated SQL
        validate_sql(sql, schema)

    except Exception as e:
        # Attempt a single repair pass if validation fails
        try:
            sql = repair_sql(
                invalid_sql=sql,
                error_message=str(e),
                schema=schema,
                user_query=user_query
            )
            validate_sql(sql, schema)
        except Exception as repair_error:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "SQL generation/repair failed",
                    "initial_error": str(e),
                    "repair_error": str(repair_error),
                    "candidate_sql": sql
                }
            )

    # Execute the final SQL
    try:
        db_url = f"mysql+pymysql://{quote_plus(DATABASES[db_name]['user'])}:{quote_plus(DATABASES[db_name]['password'])}@{DATABASES[db_name]['host']}:{DATABASES[db_name]['port']}/{DATABASES[db_name]['database']}"
        data = execute_sql(db_url, sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {str(e)}")

    return {
        "sql": sql,
        "data": data
    }
