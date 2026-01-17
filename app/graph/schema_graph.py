import networkx as nx
from sqlalchemy import create_engine, text


def build_schema_context(db_url: str) -> dict:
    """
    Builds:
    - Table -> columns mapping
    - Foreign-key graph with join metadata
    """

    engine = create_engine(db_url)
    graph = nx.DiGraph()
    tables = {}

    with engine.connect() as conn:
        # --- Load columns ---
        cols = conn.execute(
            text("""
                SELECT
                    table_name  AS table_name,
                    column_name AS column_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
            """)
        ).mappings()

        for row in cols:
            tables.setdefault(row["table_name"], []).append(row["column_name"])

        # --- Load foreign keys ---
        fks = conn.execute(
            text("""
                SELECT
                    table_name               AS table_name,
                    referenced_table_name    AS referenced_table,
                    column_name              AS column_name,
                    referenced_column_name   AS referenced_column
                FROM information_schema.key_column_usage
                WHERE table_schema = DATABASE()
                  AND referenced_table_name IS NOT NULL
            """)
        ).mappings()

        for row in fks:
            graph.add_edge(
                row["referenced_table"],
                row["table_name"],
                join_on=(
                    f"{row['referenced_table']}.{row['referenced_column']}",
                    f"{row['table_name']}.{row['column_name']}"
                )
            )

    return {
        "tables": tables,
        "graph": graph
    }
