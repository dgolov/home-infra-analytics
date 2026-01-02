from pydantic import BaseModel, Field

from typing import Dict


class Metric(BaseModel):
    host: str
    vm: str
    metric: str
    value: float
    tags: Dict[str, str] = Field(default={})
