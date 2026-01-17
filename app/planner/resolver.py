import networkx as nx
from datetime import date, timedelta

# Map canonical entity names to actual table names
ENTITIES = {
    "Customer": "customer",
    "Film": "film",
    "Actor": "actor",
    "Inventory": "inventory",
    "Store": "store",
    # Add any additional canonical names here
}

# Map common synonyms from natural language to canonical names
ENTITY_ALIASES = {
    "Movie": "Film",
    "Movies": "Film",
    "Users": "Customer",
    "Clients": "Customer",
}

# Example metrics for demonstration
METRICS = {
    "Revenue": {
        "expression": "SUM(payment.amount)",
        "table": "payment",
        "time_column": "payment_date"
    },
    "Count": {
        "expression": "COUNT(*)",
        "table": None,  # same as base table
        "time_column": None
    }
}


def resolve_query(ir: dict, graph: nx.DiGraph):
    # Normalize entity name
    entity_name = ENTITY_ALIASES.get(ir["entity"], ir["entity"])
    if entity_name not in ENTITIES:
        raise ValueError(f"Unknown entity: {entity_name}")
    base_table = ENTITIES[entity_name]

    # Determine metric
    metric_name = ir.get("metric", "Count")  # default to Count if not provided
    metric_info = METRICS.get(metric_name)
    if not metric_info:
        raise ValueError(f"Unknown metric: {metric_name}")

    required_table = metric_info["table"] or base_table

    # Compute joins if necessary
    joins = []
    if required_table != base_table:
        path = nx.shortest_path(graph, base_table, required_table)
        for i in range(len(path) - 1):
            edge = graph.edges[path[i], path[i + 1]]
            joins.append(edge["join_on"])

    # Filters
    filters = []
    if ir.get("filters"):
        today = date.today()
        start = today.replace(day=1) - timedelta(days=30)
        if metric_info.get("time_column"):
            filters.append(
                f"{required_table}.{metric_info['time_column']} BETWEEN '{start}' AND '{today}'"
            )

    # Build select
    select = [
        f"{base_table}.id" if "id" in base_table else f"{base_table}.{base_table}_id",
        f"{metric_info['expression']} AS value"
    ]

    # Assemble plan
    return {
        "base_table": base_table,
        "joins": joins,
        "select": select,
        "filters": filters,
        "group_by": [select[0]],
        "order_by": ["value DESC"],
        "limit": ir.get("limit", 10)
    }
