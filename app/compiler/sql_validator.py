import re

FORBIDDEN = {"insert", "update", "delete", "drop", "alter"}

def extract_tables_and_aliases(sql: str):
    tables = {}
    matches = re.findall(r"(from|join)\s+(\w+)(?:\s+as)?\s*(\w+)?", sql, re.I)
    for _, table, alias in matches:
        tables[alias or table] = table
    return tables

def extract_columns(sql: str):
    return re.findall(r"(\w+)\.(\w+)", sql)

def validate_sql(sql: str, schema: dict):
    lowered = sql.lower()
    if any(word in lowered for word in FORBIDDEN):
        raise ValueError("Forbidden SQL operation detected")
    if ";" in sql.strip()[:-1]:
        raise ValueError("Multiple SQL statements detected")

    tables = extract_tables_and_aliases(sql)
    columns = extract_columns(sql)
    schema_tables = schema["tables"]

    # Table validation
    for alias, table in tables.items():
        if table not in schema_tables:
            raise ValueError(f"Unknown table: {table}")

    # Column validation
    for alias, col in columns:
        if alias not in tables:
            raise ValueError(f"Unknown table alias: {alias}")
        table = tables[alias]
        if col not in schema_tables[table]:
            raise ValueError(f"Unknown column: {table}.{col}")

    # Join validation
    graph = schema["graph"]
    joins = re.findall(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", sql)
    for a1, c1, a2, c2 in joins:
        t1, t2 = tables[a1], tables[a2]
        valid = False
        if graph.has_edge(t1, t2):
            left, right = graph.edges[t1, t2]["join_on"]
            valid |= (left == f"{t1}.{c1}" and right == f"{t2}.{c2}")
        if graph.has_edge(t2, t1):
            left, right = graph.edges[t2, t1]["join_on"]
            valid |= (left == f"{t2}.{c2}" and right == f"{t1}.{c1}")
        if not valid:
            raise ValueError(f"Invalid join: {t1}.{c1} = {t2}.{c2}")

    return True
