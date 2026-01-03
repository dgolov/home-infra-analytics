from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, RootModel

from typing import Dict, List


class Metric(BaseModel):
    host: str
    vm: str
    metric: str
    value: float
    tags: Dict[str, str] = Field(default={})


class MetricBatch(RootModel[List[Metric]]):
    ...


class Scope(str, Enum):
    vm = "vm"
    host = "host"
    global_ = "global"


class MetricsQuery(BaseModel):
    metric: str
    scope: Scope
    host: str | None = None
    vm: str | None = None
    from_ts: datetime
    to_ts: datetime
