from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, RootModel, model_validator

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
    vm: str = "vm"
    host: str = "host"
    global_: str = "global"


class CardinalityScope(str, Enum):
    vm: str = "vm"
    host: str = "host"
    metric: str = "metric"


class Resolution(str, Enum):
    m1: str = "1m"
    m5: str = "5m"
    h1: str = "1h"


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


class MetricsTrendQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Field(default=Resolution.m1)

    from_ts: datetime
    to_ts: datetime

    host: Optional[str] = Field(default=None)
    vm: Optional[str] = Field(default=None)

    @model_validator(mode="after")
    def validate_scope(self):
        if self.scope == Scope.host and not self.host:
            raise ValueError("host is required when scope=host")

        if self.scope == Scope.vm:
            if not self.host or not self.vm:
                raise ValueError("host and vm are required when scope=vm")

        return self


class MetricsBottomQuery(BaseModel):
    metric: str
    scope: Scope
    resolution: Resolution = Field(default=Resolution.m1)
    limit: int = 10

    host: Optional[str] = Field(default=None)


class MetricsExtremesQuery(BaseModel):
    resolution: Resolution = Resolution.m1
    limit: int = 5

    from_ts: datetime
    to_ts: datetime
