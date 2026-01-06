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
    vm: str = Field(default="vm")
    host: str = Field(default="host")
    global_: str = Field(default="global")


class CardinalityScope(str, Enum):
    vm: str = Field(default="vm")
    host: str = Field(default="host")
    metric: str = Field(default="metric")


class Resolution(str, Enum):
    m1: str = Field(default="1m")
    m5: str = Field(default="5m")
    h1: str = Field(default="1h")


class MetricsQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Field(default=Resolution.m1)

    host: Optional[str] = Field(default=None)
    vm: Optional[str] = Field(default=None)

    from_ts: datetime
    to_ts: datetime


class LatestMetricsQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Field(default=Resolution.m1)

    host: Optional[str] = Field(default=None)
    vm: Optional[str] = Field(default=None)


class MetricsTopQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Field(default=Resolution.m1)
    limit: int = 10

    host: Optional[str] = Field(default=None)


class MetricsCompareQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Field(default=Resolution.m1)

    from_a: datetime
    to_a: datetime
    from_b: datetime
    to_b: datetime

    host: Optional[str] = Field(default=None)
    vm: Optional[str] = Field(default=None)


class MetricsCardinalityQuery(BaseModel):
    scope: CardinalityScope
    resolution: Resolution = Field(default=Resolution.m1)

    from_ts: Optional[datetime] = Field(default=None)
    to_ts: Optional[datetime] = Field(default=None)
