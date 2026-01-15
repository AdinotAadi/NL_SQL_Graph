import networkx as nx
from sqlalchemy import create_engine, text


def build_schema_graph(db_url: str) -> nx.DiGraph:
    """
    Builds a directed schema graph from MySQL foreign key metadata.

    Edge direction:
        referenced_table  -->  table_with_fk

    Edge attribute:
        join_on = (left_column, right_column)
    """
    engine = create_engine(db_url)
    graph = nx.DiGraph()

    with engine.connect() as conn:
        result = conn.execute(
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

        for row in result:
            referenced_table = row["referenced_table"]
            table = row["table_name"]
            referenced_column = row["referenced_column"]
            column = row["column_name"]

            graph.add_edge(
                referenced_table,
                table,
                join_on=(
                    f"{referenced_table}.{referenced_column}",
                    f"{table}.{column}"
                )
            )

    return graph
