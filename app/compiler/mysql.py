def compile_sql(plan: dict) -> str:
    """
    Builds a SQL query from a resolved plan dictionary.
    """
    sql = "SELECT " + ", ".join(plan["select"])
    sql += f" FROM {plan['base_table']}"

    for left, right in plan["joins"]:
        # Ensure we join the correct table
        right_table = right.split(".")[0]
        sql += f" JOIN {right_table} ON {left} = {right}"

    if plan["filters"]:
        sql += " WHERE " + " AND ".join(plan["filters"])

    if plan.get("group_by"):
        sql += " GROUP BY " + ", ".join(plan["group_by"])

    if plan.get("order_by"):
        sql += " ORDER BY " + ", ".join(plan["order_by"])

    if plan.get("limit"):
        sql += f" LIMIT {plan['limit']}"

    return sql
