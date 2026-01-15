from pydantic import BaseModel
from typing import List, Optional

class TimeFilter(BaseModel):
    value: str  # e.g. "last_month"

class OrderBy(BaseModel):
    metric: str
    direction: str  # asc / desc

class QueryIR(BaseModel):
    entity: str
    metric: str
    filters: Optional[List[TimeFilter]] = []
    group_by: Optional[List[str]] = []
    order_by: Optional[List[OrderBy]] = []
    limit: Optional[int] = 10
