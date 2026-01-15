def compile_sql(plan: dict) -> str:
    sql = "SELECT " + ", ".join(plan["select"])
    sql += f" FROM {plan['base_table']}"

    for left, right in plan["joins"]:
        sql += f" JOIN {right.split('.')[0]} ON {left} = {right}"

    if plan["filters"]:
        sql += " WHERE " + " AND ".join(plan["filters"])

    sql += " GROUP BY " + ", ".join(plan["group_by"])
    sql += " ORDER BY " + ", ".join(plan["order_by"])
    sql += f" LIMIT {plan['limit']}"

    return sql
