from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, RootModel

from typing import Dict, List, Optional


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


class CardinalityScope(str, Enum):
    vm = "vm"
    host = "host"
    metric = "metric"


class Resolution(str, Enum):
    m1 = "1m"
    m5 = "5m"
    h1 = "1h"


class MetricsQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Resolution.m1

    host: str | None = None
    vm: str | None = None

    from_ts: datetime
    to_ts: datetime


class LatestMetricsQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Resolution.m1

    host: str | None = None
    vm: str | None = None


class MetricsTopQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Resolution.m1
    limit: int = 10

    host: str | None = None


class MetricsCompareQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Resolution.m1

    from_a: datetime
    to_a: datetime
    from_b: datetime
    to_b: datetime

    host: Optional[str] = None
    vm: Optional[str] = None


class MetricsCardinalityQuery(BaseModel):
    scope: CardinalityScope
    resolution: Resolution = Resolution.m1

    from_ts: Optional[datetime] = None
    to_ts: Optional[datetime] = None
