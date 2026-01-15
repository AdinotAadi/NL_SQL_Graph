import networkx as nx
from datetime import date, timedelta

SEMANTIC_MAP = {
    "Customer": "customer",
    "Film": "film",
    "Actor": "actor",
    "Rental": "rental",

    "Revenue": {
        "expression": "SUM(payment.amount)",
        "table": "payment",
        "time_column": "payment_date"
    },
    "Count": {
        "expression": "COUNT(*)",
        "table": None
    }
}


def resolve_query(ir: dict, graph: nx.DiGraph) -> dict:
    if "entity" not in ir or "metric" not in ir:
        raise ValueError("IR must contain entity and metric")

    entity_table = SEMANTIC_MAP[ir["entity"]]
    metric_info = SEMANTIC_MAP[ir["metric"]]

    required_table = metric_info.get("table") or entity_table

    path = nx.shortest_path(graph, entity_table, required_table)

    joins = []
    for i in range(len(path) - 1):
        edge = graph.edges[path[i], path[i + 1]]
        joins.append(edge["join_on"])

    filters = []
    if ir.get("time_range") == "last_month" and "time_column" in metric_info:
        today = date.today()
        start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        filters.append(
            f"{required_table}.{metric_info['time_column']} "
            f"BETWEEN '{start}' AND '{today}'"
        )

    return {
        "base_table": entity_table,
        "joins": joins,
        "select": [
            f"{entity_table}.{entity_table}_id",
            f"{metric_info['expression']} AS value"
        ],
        "filters": filters,
        "group_by": [f"{entity_table}.{entity_table}_id"],
        "order_by": ["value DESC"],
        "limit": ir.get("limit", 10)
    }
