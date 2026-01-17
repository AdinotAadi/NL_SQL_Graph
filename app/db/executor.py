from sqlalchemy import create_engine, text

def execute_sql(db_url: str, sql: str):
    """
    Executes a SQL query on the given database URL.
    Returns results as a list of dictionaries.
    """
    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return [dict(row) for row in result.mappings()]
