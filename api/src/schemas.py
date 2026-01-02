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
